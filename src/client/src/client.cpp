#include <algorithm>
#include <dfg/atomic_output.hpp>
#include <dfg/client/client.hpp>
#include <dfg/net_utils.hpp>
#include <dfg/protocol.hpp>
#include <dfg/sha256.hpp>
#include <dfg/thread_pool.hpp>
#include <dfg/unique_fd.hpp>
#include <fstream>
#include <future>
#include <iostream>
#include <map>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace dfg::client {
static std::string calculate_checksum(const std::vector<char> &data) {
  return dfg::hash::sha256(data);
}

constexpr size_t DEFAULT_CHUNK_SIZE = 64 * 1024 * 1024; // 64 MB chunk size

int file_delete(std::string server_ip, int server_port, std::string file_name) {
  if (!dfg::protocol::valid_name(file_name)) {
    std::cerr << "Invalid remote filename" << std::endl;
    return 1;
  }

  int sock = dfg::net::connect_with_timeout(server_ip, server_port);
  dfg::UniqueFd socket_owner(sock);
  if (sock < 0) {
    std::cerr << "Cannot connect to head server at " << server_ip << ":"
              << server_port << std::endl;
    return 1;
  }

  std::string req = "DELETE " + file_name + "\n";
  if (!dfg::net::send_all(sock, req.data(), req.size()))
    return 1;

  std::string resp;
  if (dfg::net::recv_line(sock, resp) && resp == "SUCCESS") {
    std::cout << "Delete successful\n";
    return 0;
  }
  std::cerr << "Delete failed: "
            << (resp.empty() ? "head server disconnected" : resp) << std::endl;
  return 1;
}

// Fetches one verified chunk. The caller catches exceptions before committing
// the destination, so a failed download never replaces an existing local file.
struct DownloadChunk {
  uint64_t id;
  std::string host;
  uint64_t port;
  std::string digest;
};

static std::vector<char> fetch_chunk(const DownloadChunk &chunk,
                                     const std::string &filename) {
  dfg::UniqueFd connection(
      dfg::net::connect_with_timeout(chunk.host, chunk.port));
  if (connection.get() < 0)
    throw std::runtime_error("Cannot connect to storage node");
  const std::string request =
      "GET_CHUNK " + std::to_string(chunk.id) + " " + filename + "\n";
  if (!dfg::net::send_all(connection.get(), request.data(), request.size()))
    throw std::runtime_error("Cannot request chunk");
  std::string reply;
  if (!dfg::net::recv_line(connection.get(), reply) ||
      !reply.starts_with("CHUNK_SIZE "))
    throw std::runtime_error("Invalid chunk response: " + reply);
  uint64_t size = 0;
  if (!dfg::protocol::parse_size(reply.substr(11), size) || size == 0 ||
      size > dfg::protocol::max_chunk_bytes)
    throw std::runtime_error("Invalid chunk size");
  std::vector<char> data(size);
  if (!dfg::net::recv_all(connection.get(), data.data(), data.size()))
    throw std::runtime_error("Incomplete chunk data");
  if (dfg::hash::sha256(data) != chunk.digest)
    throw std::runtime_error("Chunk checksum mismatch");
  return data;
}

int file_download(std::string server_ip, int server_port, std::string file_name,
                  std::string output_path) {
  try {
    if (!dfg::protocol::valid_name(file_name))
      throw std::runtime_error("Invalid remote filename");
    dfg::UniqueFd connection(
        dfg::net::connect_with_timeout(server_ip, server_port));
    if (connection.get() < 0)
      throw std::runtime_error("Cannot connect to head server");
    const std::string request = "DOWNLOAD " + file_name + "\n";
    if (!dfg::net::send_all(connection.get(), request.data(), request.size()))
      throw std::runtime_error("Cannot request download");

    std::string file_digest;
    std::optional<uint64_t> expected_chunks;
    std::map<uint64_t, DownloadChunk> chunks;
    for (;;) {
      std::string line;
      if (!dfg::net::recv_line(connection.get(), line))
        throw std::runtime_error("Incomplete download metadata");
      if (line == "EOF")
        break;
      if (line.starts_with("ERROR:"))
        throw std::runtime_error(line);
      if (line.starts_with("FILE_HASH ")) {
        const auto digest = line.substr(10);
        if (!dfg::protocol::valid_digest(digest) ||
            (!file_digest.empty() && file_digest != digest))
          throw std::runtime_error("Invalid or conflicting file hash");
        file_digest = digest;
      } else if (line.starts_with("CHUNKS ")) {
        uint64_t count = 0;
        if (expected_chunks ||
            !dfg::protocol::parse_size(line.substr(7), count) ||
            count > 1000000)
          throw std::runtime_error("Invalid chunk count");
        expected_chunks = count;
      } else if (line.starts_with("CHUNK_LOC ")) {
        std::istringstream fields(line.substr(10));
        std::string id, port, extra;
        DownloadChunk chunk{};
        if (!(fields >> id >> chunk.host >> port >> chunk.digest) ||
            (fields >> extra) || !dfg::protocol::parse_size(id, chunk.id) ||
            !dfg::protocol::parse_size(port, chunk.port) || chunk.port == 0 ||
            chunk.port > 65535 || !dfg::protocol::valid_digest(chunk.digest) ||
            chunks.size() >= 1000000 || !chunks.emplace(chunk.id, chunk).second)
          throw std::runtime_error("Invalid or duplicate chunk location");
      } else {
        throw std::runtime_error("Unknown download metadata: " + line);
      }
    }
    if (file_digest.empty() ||
        (expected_chunks && *expected_chunks != chunks.size()) ||
        (!chunks.empty() && (!expected_chunks || chunks.begin()->first != 0 ||
                             chunks.rbegin()->first != chunks.size() - 1)))
      throw std::runtime_error(
          "Missing file hash or incomplete chunk locations");
    connection.reset();

    dfg::AtomicOutput output(output_path);
    dfg::hash::SHA256 file_hash;
    dfg::ThreadPool downloads(4);
    auto next = chunks.begin();
    while (next != chunks.end()) {
      // A batch holds at most four chunks, independently of file size.
      std::vector<std::future<std::vector<char>>> batch;
      for (size_t count = 0; count < 4 && next != chunks.end();
           ++count, ++next) {
        const auto chunk = next->second;
        batch.push_back(downloads.submit(
            [chunk, file_name] { return fetch_chunk(chunk, file_name); }));
      }
      for (auto &result : batch) {
        auto data = result.get();
        file_hash.update(data);
        output.stream().write(data.data(), data.size());
        if (!output.stream())
          throw std::runtime_error("Failed to write downloaded data");
      }
    }
    if (file_hash.digest() != file_digest)
      throw std::runtime_error("Downloaded file checksum does not match");
    output.commit();
    std::cout << "Download successful" << std::endl;
    return 0;
  } catch (const std::exception &error) {
    std::cerr << "Download failed: " << error.what() << std::endl;
    return 1;
  }
}

int file_upload(const std::string &server_ip, int server_port,
                const std::string &file_path, const std::string &file_name) {
  if (!dfg::protocol::valid_name(file_name)) {
    std::cerr << "Invalid remote filename" << std::endl;
    return 1;
  }

  std::ifstream in_file(file_path, std::ios::binary | std::ios::ate);
  if (!in_file) {
    std::cerr << "Error: Failed to open local file for upload: " << file_path
              << std::endl;
    return 1;
  }

  std::streamsize file_size = in_file.tellg();
  if (file_size < 0) {
    std::cerr << "Cannot determine upload size" << std::endl;
    return 1;
  }
  in_file.seekg(0, std::ios::beg);

  std::cout << "Calculating SHA256 checksum for: " << file_path << " ("
            << file_size << " bytes)..." << std::endl;
  dfg::hash::SHA256 hasher;
  std::vector<char> buffer(64 * 1024);
  while (in_file) {
    in_file.read(buffer.data(), buffer.size());
    std::streamsize bytes_read = in_file.gcount();
    if (bytes_read > 0) {
      hasher.update(buffer.data(), bytes_read);
    }
  }
  if (in_file.bad()) {
    std::cerr << "Cannot read upload source" << std::endl;
    return 1;
  }
  std::string file_hash = hasher.digest();
  std::cout << "SHA256: " << file_hash << std::endl;

  // Calculate chunks
  const size_t CHUNK_SIZE = DEFAULT_CHUNK_SIZE;
  size_t num_chunks =
      (file_size == 0)
          ? 0
          : static_cast<size_t>((file_size + CHUNK_SIZE - 1) / CHUNK_SIZE);

  // Reset file pointer to beginning for sending
  in_file.clear();
  in_file.seekg(0, std::ios::beg);

  int sock = dfg::net::connect_with_timeout(server_ip, server_port);
  dfg::UniqueFd socket_owner(sock);
  if (sock < 0) {
    std::cerr << "Cannot connect to head server" << std::endl;
    return 1;
  }

  // Send UPLOAD command: UPLOAD <filename> <file_size> <num_chunks>
  // <file_hash>\n
  std::string upload_cmd = "UPLOAD " + file_name + " " +
                           std::to_string(file_size) + " " +
                           std::to_string(num_chunks) + " " + file_hash + "\n";
  if (!dfg::net::send_all(sock, upload_cmd.data(), upload_cmd.size())) {
    std::cerr << "Failed to send UPLOAD command to server" << std::endl;
    socket_owner.reset();
    return 1;
  }

  std::string ready_resp;
  if (!dfg::net::recv_line(sock, ready_resp) || ready_resp != "READY") {
    std::cerr << "Server did not respond with READY. Response: " << ready_resp
              << std::endl;
    socket_owner.reset();
    return 1;
  }

  std::cout << "Uploading file divided into " << num_chunks
            << " chunks to Head Server..." << std::endl;
  size_t total_sent = 0;
  for (size_t i = 0; i < num_chunks; ++i) {
    size_t current_chunk_size =
        std::min(CHUNK_SIZE, static_cast<size_t>(file_size - total_sent));
    std::vector<char> chunk_buffer(current_chunk_size);

    size_t read_bytes = 0;
    while (read_bytes < current_chunk_size && in_file) {
      in_file.read(chunk_buffer.data() + read_bytes,
                   current_chunk_size - read_bytes);
      std::streamsize n = in_file.gcount();
      if (n <= 0)
        break;
      read_bytes += n;
    }

    if (read_bytes != current_chunk_size) {
      std::cerr << "Failed to read full chunk " << i << " from file"
                << std::endl;
      socket_owner.reset();
      return 1;
    }

    std::string chunk_hash = calculate_checksum(chunk_buffer);
    std::string chunk_hdr = "CHUNK " + std::to_string(i) + " " +
                            std::to_string(current_chunk_size) + " " +
                            chunk_hash + "\n";
    if (!dfg::net::send_all(sock, chunk_hdr.data(), chunk_hdr.size())) {
      std::cerr << "Failed to send chunk " << i << " header to server"
                << std::endl;
      socket_owner.reset();
      return 1;
    }

    if (!dfg::net::send_all(sock, chunk_buffer.data(), current_chunk_size)) {
      std::cerr << "Failed to send chunk " << i << " data to server"
                << std::endl;
      return 1;
    }
    total_sent += current_chunk_size;
    std::cout << "Sent chunk " << i << " (" << current_chunk_size
              << " bytes, hash: " << chunk_hash.substr(0, 8) << "...)"
              << std::endl;
  }
  in_file.close();

  // Signal completion
  std::string eof_cmd = "EOF\n";
  if (!dfg::net::send_all(sock, eof_cmd.data(), eof_cmd.size()))
    return 1;

  std::cout << "Waiting for server confirmation..." << std::endl;
  std::string final_resp;
  if (!dfg::net::recv_line(sock, final_resp)) {
    std::cerr << "Connection closed before upload confirmation" << std::endl;
    return 1;
  }
  socket_owner.reset();

  if (final_resp.rfind("SUCCESS", 0) == 0) {
    std::cout << "File uploaded successfully: " << file_name << std::endl;
    return 0;
  } else {
    std::cerr << "Server returned error on upload: " << final_resp << std::endl;
    return 1;
  }
}

int file_list(const std::string &server_ip, int server_port) {
  int sock = dfg::net::connect_with_timeout(server_ip, server_port);
  dfg::UniqueFd socket_owner(sock);
  if (sock < 0) {
    std::cerr << "Cannot connect to head server" << std::endl;
    return 1;
  }

  std::string req = "LIST\n";
  if (!dfg::net::send_all(sock, req.data(), req.size())) {
    std::cerr << "Failed to send LIST command to server" << std::endl;
    socket_owner.reset();
    return 1;
  }

  std::vector<std::string> files;
  while (true) {
    std::string line;
    if (!dfg::net::recv_line(sock, line)) {
      std::cerr << "Connection closed before file listing completed"
                << std::endl;
      return 1;
    }
    if (line == "EOF") {
      break;
    }
    if (line.rfind("ERROR:", 0) == 0) {
      std::cerr << "Server Error: " << line << std::endl;
      socket_owner.reset();
      return 1;
    }
    files.push_back(line);
  }
  socket_owner.reset();

  if (files.empty()) {
    std::cout << "No files tracked in metadata store." << std::endl;
    return 0;
  }
  std::cout << "Tracked files:" << std::endl;
  for (const auto &file : files) {
    std::cout << "  - " << file << std::endl;
  }
  return 0;
}

} // namespace dfg::client
