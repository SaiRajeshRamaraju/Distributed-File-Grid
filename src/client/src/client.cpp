#include <algorithm>
#include <arpa/inet.h>
#include <cstdlib>
#include <dfg/client/client.hpp>
#include <dfg/net_utils.hpp>
#include <dfg/sha256.hpp>
#include <dfg/unique_fd.hpp>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <mutex>
#include <netinet/in.h>
#include <sstream>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace dfg::client {
static std::string calculate_checksum(const std::vector<char> &data) {
  return dfg::hash::sha256(data);
}

constexpr size_t DEFAULT_CHUNK_SIZE = 64 * 1024 * 1024; // 64 MB chunk size

int file_delete(std::string server_ip, int server_port, std::string file_name) {
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  dfg::UniqueFd socket_owner(sock);
  if (sock < 0)
    return 1;

  sockaddr_in serv_addr{};
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port = htons(server_port);
  if (inet_pton(AF_INET, server_ip.c_str(), &serv_addr.sin_addr) <= 0)
    return 1;

  if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
    return 1;

  std::string req = "DELETE " + file_name + "\n";
  if (!dfg::net::send_all(sock, req.data(), req.size()))
    return 1;

  std::string resp;
  if (dfg::net::recv_line(sock, resp) && resp == "SUCCESS") {
    std::cout << "Delete successful\n";
    return 0;
  }
  std::cerr << "error\n";
  return 1;
}

int file_download(std::string server_ip, int server_port, std::string file_name,
                  std::string output_path) {
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  dfg::UniqueFd socket_owner(sock);
  if (sock < 0) {
    std::cerr << "Socket creation error" << std::endl;
    return 1;
  }

  sockaddr_in serv_addr{};
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port = htons(server_port);
  if (inet_pton(AF_INET, server_ip.c_str(), &serv_addr.sin_addr) <= 0) {
    std::cerr << "Invalid address/ Address not supported" << std::endl;
    return 1;
  }

  if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
    std::cerr << "Connection Failed" << std::endl;
    return 1;
  }

  std::string req = "DOWNLOAD " + file_name + "\n";
  if (!dfg::net::send_all(sock, req.data(), req.size()))
    return 1;

  std::string file_hash_from_server;

  struct ChunkInfo {
    int id;
    std::string ip;
    int port;
    std::string hash;
  };
  std::vector<ChunkInfo> chunks;

  while (true) {
    std::string line;
    if (!dfg::net::recv_line(sock, line) || line.empty()) {
      std::cerr << "Connection closed by server prematurely." << std::endl;
      return 1;
    }

    if (line.rfind("ERROR:", 0) == 0) {
      std::cerr << "Server Error: " << line << std::endl;
      return 1;
    }

    if (line.rfind("FILE_HASH ", 0) == 0) {
      file_hash_from_server = line.substr(10);
      std::cout << "Received file hash from server: " << file_hash_from_server
                << std::endl;
    } else if (line.rfind("CHUNKS ", 0) == 0) {
      std::cout << "File has " << line.substr(7) << " chunk(s) to download."
                << std::endl;
    } else if (line.rfind("CHUNK_LOC ", 0) == 0) {
      std::istringstream iss(line.substr(10));
      ChunkInfo info;
      if (!(iss >> info.id >> info.ip >> info.port >> info.hash)) {
        std::cerr << "Invalid chunk_loc header: " << line << std::endl;
        return 1;
      }
      chunks.push_back(info);
    } else if (line == "EOF") {
      break;
    } else {
      std::cerr << "Unknown response: " << line << std::endl;
    }
  }
  socket_owner.reset();

  // Parallel download
  std::cout << "Starting parallel download of " << chunks.size() << " chunks..."
            << std::endl;
  std::vector<std::thread> threads;
  std::mutex mtx;
  bool download_error = false;
  std::map<int, std::vector<char>> chunk_data;

  for (const auto &chunk : chunks) {
    threads.emplace_back([&, chunk]() {
      int csock = socket(AF_INET, SOCK_STREAM, 0);
      dfg::UniqueFd chunk_socket_owner(csock);
      if (csock < 0) {
        std::lock_guard<std::mutex> lock(mtx);
        std::cerr << "\033[31mError: socket creation failed for chunk "
                  << chunk.id << "\033[0m" << std::endl;
        download_error = true;
        return;
      }
      sockaddr_in caddr{};
      caddr.sin_family = AF_INET;
      caddr.sin_port = htons(chunk.port);
      if (inet_pton(AF_INET, chunk.ip.c_str(), &caddr.sin_addr) <= 0) {
        std::lock_guard<std::mutex> lock(mtx);
        std::cerr << "\033[31mError: invalid IP for chunk " << chunk.id
                  << "\033[0m" << std::endl;
        download_error = true;
        chunk_socket_owner.reset();
        return;
      }
      if (connect(csock, (struct sockaddr *)&caddr, sizeof(caddr)) < 0) {
        std::lock_guard<std::mutex> lock(mtx);
        std::cerr << "\033[31mError: connection failed for chunk " << chunk.id
                  << "\033[0m" << std::endl;
        download_error = true;
        chunk_socket_owner.reset();
        return;
      }

      std::string get_req =
          "GET_CHUNK " + std::to_string(chunk.id) + " " + file_name + "\n";
      if (!dfg::net::send_all(csock, get_req.data(), get_req.size())) {
        std::lock_guard<std::mutex> lock(mtx);
        download_error = true;
        return;
      }

      std::string resp;
      if (!dfg::net::recv_line(csock, resp) ||
          resp.rfind("CHUNK_SIZE ", 0) != 0) {
        std::lock_guard<std::mutex> lock(mtx);
        std::cerr << "\033[31mError: server returned error for chunk "
                  << chunk.id << ": " << resp << "\033[0m" << std::endl;
        download_error = true;
        chunk_socket_owner.reset();
        return;
      }
      size_t size = 0;
      std::istringstream size_header(resp.substr(11));
      if (!(size_header >> size)) {
        std::lock_guard<std::mutex> lock(mtx);
        std::cerr << "Invalid chunk size for chunk " << chunk.id << std::endl;
        download_error = true;
        return;
      }
      std::vector<char> buf(size);
      bool complete = dfg::net::recv_all(csock, buf.data(), size);
      chunk_socket_owner.reset();

      if (!complete) {
        std::lock_guard<std::mutex> lock(mtx);
        std::cerr << "\033[31mError: incomplete data for chunk " << chunk.id
                  << "\033[0m" << std::endl;
        download_error = true;
        return;
      }

      std::string client_hash = calculate_checksum(buf);
      std::lock_guard<std::mutex> lock(mtx);
      if (client_hash != chunk.hash) {
        std::cerr << "\033[31mError: Corrupted chunk received!\n"
                  << "Order ID: " << chunk.id << "\n"
                  << "Current Hash (Generated): " << client_hash << "\n"
                  << "Expected Hash (from Head Server): " << chunk.hash
                  << "\033[0m" << std::endl;
        download_error = true;
      } else {
        std::cout << "\033[32mSuccessfully received and verified chunk "
                  << chunk.id << "\033[0m" << std::endl;
        chunk_data[chunk.id] = std::move(buf);
      }
    });
  }

  for (auto &t : threads)
    t.join();

  if (download_error) {
    std::cerr << "Download failed: one or more chunks could not be verified"
              << std::endl;
    return 1;
  }

  std::ofstream out_file(output_path, std::ios::binary);
  if (!out_file) {
    std::cerr << "Failed to open output file" << std::endl;
    return 1;
  }

  dfg::hash::SHA256 full_file_hash;
  for (size_t i = 0; i < chunks.size(); ++i) {
    auto &data = chunk_data[i];
    full_file_hash.update(data);
    out_file.write(data.data(), data.size());
  }
  out_file.close();

  std::string final_generated_hash = full_file_hash.digest();

  if (!file_hash_from_server.empty() &&
      final_generated_hash != file_hash_from_server) {
    std::cerr << "Downloaded file checksum does not match" << std::endl;
    remove(output_path.c_str());
    return 1;
  }

  std::cout << "Download successful" << std::endl;
  return 0;
}

int file_upload(const std::string &server_ip, int server_port,
                const std::string &file_path, const std::string &file_name) {
  std::ifstream in_file(file_path, std::ios::binary | std::ios::ate);
  if (!in_file) {
    std::cerr << "Error: Failed to open local file for upload: " << file_path
              << std::endl;
    return 1;
  }

  std::streamsize file_size = in_file.tellg();
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

  int sock = socket(AF_INET, SOCK_STREAM, 0);
  dfg::UniqueFd socket_owner(sock);
  if (sock < 0) {
    std::cerr << "Socket creation error" << std::endl;
    return 1;
  }

  sockaddr_in serv_addr{};
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port = htons(server_port);
  if (inet_pton(AF_INET, server_ip.c_str(), &serv_addr.sin_addr) <= 0) {
    std::cerr << "Invalid address / Address not supported: " << server_ip
              << std::endl;
    socket_owner.reset();
    return 1;
  }

  if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
    std::cerr << "Connection to Head Server failed: " << server_ip << ":"
              << server_port << std::endl;
    socket_owner.reset();
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
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  dfg::UniqueFd socket_owner(sock);
  if (sock < 0) {
    std::cerr << "Socket creation error" << std::endl;
    return 1;
  }

  sockaddr_in serv_addr{};
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port = htons(server_port);
  if (inet_pton(AF_INET, server_ip.c_str(), &serv_addr.sin_addr) <= 0) {
    std::cerr << "Invalid address / Address not supported: " << server_ip
              << std::endl;
    socket_owner.reset();
    return 1;
  }

  if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
    std::cerr << "Connection to Head Server failed: " << server_ip << ":"
              << server_port << std::endl;
    socket_owner.reset();
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
