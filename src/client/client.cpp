#include <thread>
#include <mutex>
#include <map>
#include "dfg/sha256.hpp"
#include <dfg/version.hpp>
#include <argparse/argparse.hpp>
#include <arpa/inet.h>
#include <cstdlib>
#include <algorithm>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <netinet/in.h>
#include <sstream>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

std::string calculate_checksum(const std::vector<char> &data) {
  return dfg::hash::sha256(data);
}

constexpr size_t DEFAULT_CHUNK_SIZE = 64 * 1024 * 1024; // 64 MB chunk size





int file_delete(std::string server_ip, int server_port, std::string file_name) {

  int sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0) return 1;

  sockaddr_in serv_addr;
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port = htons(server_port);
  if (inet_pton(AF_INET, server_ip.c_str(), &serv_addr.sin_addr) <= 0) return 1;

  if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) return 1;

  std::string req = "DELETE " + file_name + "\n";
  send(sock, req.c_str(), req.length(), 0);

  char buf[256] = {0};
  ssize_t n = recv(sock, buf, sizeof(buf)-1, 0);
  close(sock);

  if (n > 0) {
    std::string resp(buf, n);
    if (resp.find("SUCCESS") != std::string::npos) {
      std::cout << "delete success\n";
      return 0;
    }
  }
  std::cerr << "error\n";
  return 1;
}

int file_download(std::string server_ip, int server_port, std::string file_name,

                  std::string output_path) {
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0) {
    std::cerr << "Socket creation error" << std::endl;
    return 1;
  }

  sockaddr_in serv_addr;
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
  send(sock, req.c_str(), req.length(), 0);

  std::string file_hash_from_server;

  auto read_line = [&](int fd) -> std::string {
    std::string line;
    char c;
    while (recv(fd, &c, 1, 0) == 1) {
      if (c == '\n')
        break;
      line += c;
    }
    return line;
  };

  struct ChunkInfo {
    int id;
    std::string ip;
    int port;
    std::string hash;
  };
  std::vector<ChunkInfo> chunks;

  while (true) {
    std::string line = read_line(sock);
    if (line.empty()) {
      std::cerr << "Connection closed by server prematurely." << std::endl;
      return 1;
    }

    if (line.rfind("ERROR:", 0) == 0) {
      std::cerr << "Server Error: " << line << std::endl;
      return 1;
    }

    if (line.rfind("FILE_HASH ", 0) == 0) {
      file_hash_from_server = line.substr(10);
      std::cout << "Received file hash from server: " << file_hash_from_server << std::endl;
    } else if (line.rfind("CHUNKS ", 0) == 0) {
      std::cout << "File has " << line.substr(7) << " chunk(s) to download." << std::endl;
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
  close(sock);

  // Parallel download
  std::cout << "Starting parallel download of " << chunks.size() << " chunks..." << std::endl;
  std::vector<std::thread> threads;
  std::mutex mtx;
  bool download_error = false;
  std::map<int, std::vector<char>> chunk_data;

  for (const auto& chunk : chunks) {
    threads.emplace_back([&, chunk]() {
      int csock = socket(AF_INET, SOCK_STREAM, 0);
      if (csock < 0) {
        std::lock_guard<std::mutex> lock(mtx);
        std::cerr << "\033[31mError: socket creation failed for chunk " << chunk.id << "\033[0m" << std::endl;
        download_error = true;
        return;
      }
      sockaddr_in caddr;
      caddr.sin_family = AF_INET;
      caddr.sin_port = htons(chunk.port);
      if (inet_pton(AF_INET, chunk.ip.c_str(), &caddr.sin_addr) <= 0) {
        std::lock_guard<std::mutex> lock(mtx);
        std::cerr << "\033[31mError: invalid IP for chunk " << chunk.id << "\033[0m" << std::endl;
        download_error = true;
        close(csock);
        return;
      }
      if (connect(csock, (struct sockaddr *)&caddr, sizeof(caddr)) < 0) {
        std::lock_guard<std::mutex> lock(mtx);
        std::cerr << "\033[31mError: connection failed for chunk " << chunk.id << "\033[0m" << std::endl;
        download_error = true;
        close(csock);
        return;
      }

      std::string get_req = "GET_CHUNK " + std::to_string(chunk.id) + " " + file_name + "\n";
      send(csock, get_req.c_str(), get_req.length(), 0);

      auto cread_line = [&](int fd) -> std::string {
        std::string line;
        char c;
        while (recv(fd, &c, 1, 0) == 1) {
          if (c == '\n') break;
          line += c;
        }
        return line;
      };

      std::string resp = cread_line(csock);
      if (resp.rfind("CHUNK_SIZE ", 0) != 0) {
        std::lock_guard<std::mutex> lock(mtx);
        std::cerr << "\033[31mError: server returned error for chunk " << chunk.id << ": " << resp << "\033[0m" << std::endl;
        download_error = true;
        close(csock);
        return;
      }
      size_t size = std::stoull(resp.substr(11));
      std::vector<char> buf(size);
      size_t total = 0;
      while (total < size) {
        ssize_t n = recv(csock, buf.data() + total, size - total, 0);
        if (n <= 0) break;
        total += n;
      }
      close(csock);

      if (total != size) {
        std::lock_guard<std::mutex> lock(mtx);
        std::cerr << "\033[31mError: incomplete data for chunk " << chunk.id << "\033[0m" << std::endl;
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
        std::cout << "\033[32mSuccessfully received and verified chunk " << chunk.id << "\033[0m" << std::endl;
        chunk_data[chunk.id] = std::move(buf);
      }
    });
  }

  for (auto& t : threads) t.join();

  if (download_error) {
    std::cerr << "error recieved chunks are corrupted" << std::endl;
    return 1;
  }

  std::ofstream out_file(output_path, std::ios::binary);
  if (!out_file) {
    std::cerr << "Failed to open output file" << std::endl;
    return 1;
  }

  dfg::hash::SHA256 full_file_hash;
  for (size_t i = 0; i < chunks.size(); ++i) {
    auto& data = chunk_data[i];
    full_file_hash.update(data);
    out_file.write(data.data(), data.size());
  }
  out_file.close();

  std::string final_generated_hash = full_file_hash.digest();

  if (!file_hash_from_server.empty() && final_generated_hash != file_hash_from_server) {
    std::cerr << "recieved data is curropted" << std::endl;
    remove(output_path.c_str());
    return 1;
  }

  std::cout << "download succesful" << std::endl;
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
  size_t num_chunks = (file_size == 0)
                          ? 0
                          : static_cast<size_t>((file_size + CHUNK_SIZE - 1) / CHUNK_SIZE);

  // Reset file pointer to beginning for sending
  in_file.clear();
  in_file.seekg(0, std::ios::beg);

  int sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0) {
    std::cerr << "Socket creation error" << std::endl;
    return 1;
  }

  sockaddr_in serv_addr;
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port = htons(server_port);
  if (inet_pton(AF_INET, server_ip.c_str(), &serv_addr.sin_addr) <= 0) {
    std::cerr << "Invalid address / Address not supported: " << server_ip
              << std::endl;
    close(sock);
    return 1;
  }

  if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
    std::cerr << "Connection to Head Server failed: " << server_ip << ":"
              << server_port << std::endl;
    close(sock);
    return 1;
  }

  // Send UPLOAD command: UPLOAD <filename> <file_size> <num_chunks> <file_hash>\n
  std::string upload_cmd = "UPLOAD " + file_name + " " +
                           std::to_string(file_size) + " " +
                           std::to_string(num_chunks) + " " +
                           file_hash + "\n";
  if (send(sock, upload_cmd.c_str(), upload_cmd.length(), 0) < 0) {
    std::cerr << "Failed to send UPLOAD command to server" << std::endl;
    close(sock);
    return 1;
  }

  auto read_line = [&](int fd) -> std::string {
    std::string line;
    char c;
    while (recv(fd, &c, 1, 0) == 1) {
      if (c == '\n')
        break;
      line += c;
    }
    return line;
  };

  std::string ready_resp = read_line(sock);
  if (ready_resp.rfind("READY", 0) != 0) {
    std::cerr << "Server did not respond with READY. Response: " << ready_resp
              << std::endl;
    close(sock);
    return 1;
  }

  std::cout << "Uploading file divided into " << num_chunks << " chunks to Head Server..." << std::endl;
  size_t total_sent = 0;
  for (size_t i = 0; i < num_chunks; ++i) {
    size_t current_chunk_size =
        std::min(CHUNK_SIZE, static_cast<size_t>(file_size - total_sent));
    std::vector<char> chunk_buffer(current_chunk_size);

    size_t read_bytes = 0;
    while (read_bytes < current_chunk_size && in_file) {
      in_file.read(chunk_buffer.data() + read_bytes, current_chunk_size - read_bytes);
      std::streamsize n = in_file.gcount();
      if (n <= 0) break;
      read_bytes += n;
    }

    if (read_bytes != current_chunk_size) {
      std::cerr << "Failed to read full chunk " << i << " from file" << std::endl;
      close(sock);
      return 1;
    }

    std::string chunk_hash = calculate_checksum(chunk_buffer);
    std::string chunk_hdr = "CHUNK " + std::to_string(i) + " " +
                            std::to_string(current_chunk_size) + " " +
                            chunk_hash + "\n";
    if (send(sock, chunk_hdr.c_str(), chunk_hdr.length(), 0) < 0) {
      std::cerr << "Failed to send chunk " << i << " header to server" << std::endl;
      close(sock);
      return 1;
    }

    size_t sent_chunk = 0;
    while (sent_chunk < current_chunk_size) {
      ssize_t n =
          send(sock, chunk_buffer.data() + sent_chunk, current_chunk_size - sent_chunk, 0);
      if (n <= 0) {
        if (n < 0 && errno == EINTR)
          continue;
        std::cerr << "Failed to send chunk " << i << " data to server" << std::endl;
        close(sock);
        return 1;
      }
      sent_chunk += n;
    }
    total_sent += current_chunk_size;
    std::cout << "Sent chunk " << i << " (" << current_chunk_size << " bytes, hash: " << chunk_hash.substr(0, 8) << "...)" << std::endl;
  }
  in_file.close();

  // Signal completion
  std::string eof_cmd = "EOF\n";
  send(sock, eof_cmd.c_str(), eof_cmd.length(), 0);

  std::cout << "Waiting for server confirmation..." << std::endl;
  std::string final_resp = read_line(sock);
  close(sock);

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
    close(sock);
    return 1;
  }

  if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
    std::cerr << "Connection to Head Server failed: " << server_ip << ":"
              << server_port << std::endl;
    close(sock);
    return 1;
  }

  std::string req = "LIST\n";
  if (send(sock, req.c_str(), req.length(), 0) < 0) {
    std::cerr << "Failed to send LIST command to server" << std::endl;
    close(sock);
    return 1;
  }

  auto read_line = [&](int fd) -> std::string {
    std::string line;
    char c;
    while (recv(fd, &c, 1, 0) == 1) {
      if (c == '\n')
        break;
      line += c;
    }
    return line;
  };

  std::vector<std::string> files;
  while (true) {
    std::string line = read_line(sock);
    if (line.empty() || line == "EOF") {
      break;
    }
    if (line.rfind("ERROR:", 0) == 0) {
      std::cerr << "Server Error: " << line << std::endl;
      close(sock);
      return 1;
    }
    files.push_back(line);
  }
  close(sock);

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

int run_client_test(const std::string &server_ip, int server_port) {
  std::cout << "Running system tests..." << std::endl;

  std::string test_file = "/tmp/dfg_test_file.txt";
  std::ofstream file(test_file);
  file << "This is a test file for the distributed storage system.\n";
  for (int i = 0; i < 1000; i++) {
    file << "Line " << i << ": Lorem ipsum dolor sit amet.\n";
  }
  file.close();

  std::cout << "\n=== Testing File Upload ===" << std::endl;
  if (file_upload(server_ip, server_port, test_file, "test_file.txt") != 0) {
    std::cout << "Upload test failed!" << std::endl;
    return -1;
  }

  std::cout << "\n=== Testing File Download ===" << std::endl;
  std::string download_path = "/tmp/dfg_downloaded_test.txt";
  if (file_download(server_ip, server_port, "test_file.txt", download_path) != 0) {
    std::cout << "Download test failed!" << std::endl;
    return -1;
  }

  std::cout << "\n=== Verifying File Integrity ===" << std::endl;
  std::ifstream original(test_file, std::ios::binary);
  std::ifstream downloaded(download_path, std::ios::binary);

  if (original && downloaded) {
    std::string orig_content((std::istreambuf_iterator<char>(original)),
                             std::istreambuf_iterator<char>());
    std::string down_content((std::istreambuf_iterator<char>(downloaded)),
                             std::istreambuf_iterator<char>());

    if (orig_content == down_content) {
      std::cout << "File integrity verified — files match!" << std::endl;
    } else {
      std::cout << "File integrity check failed — files don't match!"
                << std::endl;
      return -1;
    }
  } else {
    std::cerr << "Failed to open verification files" << std::endl;
    return -1;
  }

  std::cout << "\n=== All Tests Passed! ===" << std::endl;
  return 0;
}

static std::string get_default_server_ip() {
  const char *env = std::getenv("DFG_HEAD_HOST");
  if (!env) env = std::getenv("DFG_SERVER_HOST");
  return env ? std::string(env) : "127.0.0.1";
}

static int get_default_server_port() {
  const char *env = std::getenv("DFG_HEAD_PORT");
  if (!env) env = std::getenv("DFG_SERVER_PORT");
  if (env) {
    try {
      int p = std::stoi(env);
      if (p > 0 && p <= 65535) return p;
    } catch (...) {}
  }
  return 9669;
}

static std::string extract_filename(const std::string &path) {
  size_t last_slash = path.find_last_of("/\\");
  if (last_slash != std::string::npos) {
    return path.substr(last_slash + 1);
  }
  return path;
}

static bool validate_port(int port) {
  if (port <= 0 || port > 65535) {
    std::cerr << "Invalid port number: " << port << " (must be 1-65535)" << std::endl;
    return false;
  }
  return true;
}

static std::pair<std::string, int> resolve_server_target(
    const argparse::ArgumentParser &sub_cmd,
    const argparse::ArgumentParser &parent) {
  std::string ip = sub_cmd.is_used("--server_ip")
                       ? sub_cmd.get<std::string>("--server_ip")
                       : (parent.is_used("--server_ip")
                              ? parent.get<std::string>("--server_ip")
                              : sub_cmd.get<std::string>("--server_ip"));
  int port = sub_cmd.is_used("--server_port")
                 ? sub_cmd.get<int>("--server_port")
                 : (parent.is_used("--server_port")
                        ? parent.get<int>("--server_port")
                        : sub_cmd.get<int>("--server_port"));
  return {ip, port};
}

int main(int argc, char *argv[]) {
  argparse::ArgumentParser program("client", APP_VERSION);
  program.add_description("Distributed File Grid — Client CLI for file lifecycle operations");

  // Global / top-level flags on program
  program.add_argument("--server_ip", "--si", "-s")
      .default_value(get_default_server_ip())
      .help("Head server IP address");
  program.add_argument("--server_port", "--sp", "-p")
      .default_value(get_default_server_port())
      .scan<'i', int>()
      .help("Head server port");
  program.add_argument("--upload")
      .help("Filepath of the file to be uploaded");
  program.add_argument("--download")
      .help("Filename of the download file");
  program.add_argument("--downlaod")
      .help("Filename of the download file (alias for backward compatibility)");
  program.add_argument("-o", "--output")
      .default_value(std::string(""))
      .help("Output path of the file to be stored");
  program.add_argument("--list")
      .default_value(false)
      .implicit_value(true)
      .help("List stored files");
  program.add_argument("--test")
      .default_value(false)
      .implicit_value(true)
      .help("Run client connectivity/functionality test");

  // Subcommand: upload
  argparse::ArgumentParser upload_cmd("upload");
  upload_cmd.add_description("Upload a local file to distributed storage");
  upload_cmd.add_argument("filepath")
      .help("Path of local file to upload");
  upload_cmd.add_argument("filename")
      .default_value(std::string(""))
      .nargs(argparse::nargs_pattern::optional)
      .help("Remote filename (optional, defaults to local basename)");
  upload_cmd.add_argument("--server_ip", "--si", "-s")
      .default_value(get_default_server_ip())
      .help("Head server IP address");
  upload_cmd.add_argument("--server_port", "--sp", "-p")
      .default_value(get_default_server_port())
      .scan<'i', int>()
      .help("Head server port");

  // Subcommand: download
  argparse::ArgumentParser download_cmd("download");
  download_cmd.add_description("Download a file from distributed storage to local path");
  download_cmd.add_argument("filename")
      .help("Name of the file to download");
  download_cmd.add_argument("output_path")
      .default_value(std::string(""))
      .nargs(argparse::nargs_pattern::optional)
      .help("Local destination path (optional, defaults to filename)");
  download_cmd.add_argument("-o", "--output")
      .default_value(std::string(""))
      .help("Local destination path");
  download_cmd.add_argument("--server_ip", "--si", "-s")
      .default_value(get_default_server_ip())
      .help("Head server IP address");
  download_cmd.add_argument("--server_port", "--sp", "-p")
      .default_value(get_default_server_port())
      .scan<'i', int>()
      .help("Head server port");


  // Subcommand: list
  argparse::ArgumentParser list_cmd("list");
  list_cmd.add_description("List all files stored across the Distributed File Grid");


  // Subcommand: delete
  argparse::ArgumentParser delete_cmd("delete");
  delete_cmd.add_description("Delete a file from the distributed storage");
  delete_cmd.add_argument("filename")
      .help("Name of the file to delete");
  delete_cmd.add_argument("--server_ip", "--si", "-s")
      .default_value(get_default_server_ip())
      .help("Head server IP address");
  delete_cmd.add_argument("--server_port", "--sp", "-p")
      .default_value(get_default_server_port())
      .scan<'i', int>()
      .help("Head server port");

  list_cmd.add_argument("--server_ip", "--si", "-s")
      .default_value(get_default_server_ip())
      .help("Head server IP address");
  list_cmd.add_argument("--server_port", "--sp", "-p")
      .default_value(get_default_server_port())
      .scan<'i', int>()
      .help("Head server port");

  // Subcommand: test
  argparse::ArgumentParser test_cmd("test");
  test_cmd.add_description("Run end-to-end client connectivity and integrity test");
  test_cmd.add_argument("--server_ip", "--si", "-s")
      .default_value(get_default_server_ip())
      .help("Head server IP address");
  test_cmd.add_argument("--server_port", "--sp", "-p")
      .default_value(get_default_server_port())
      .scan<'i', int>()
      .help("Head server port");

  // Subcommand: help
  argparse::ArgumentParser help_cmd("help");
  help_cmd.add_description("Show help message");

  // Subcommand: version
  argparse::ArgumentParser version_cmd("version");
  version_cmd.add_description("Show version information");

  program.add_subparser(upload_cmd);
  program.add_subparser(download_cmd);

  program.add_subparser(list_cmd);
  program.add_subparser(delete_cmd);

  program.add_subparser(test_cmd);
  program.add_subparser(help_cmd);
  program.add_subparser(version_cmd);

  if (argc < 2) {
    std::cout << program << std::endl;
    return 1;
  }

  try {
    program.parse_args(argc, argv);
  } catch (const std::exception &err) {
    std::cerr << "Error: " << err.what() << std::endl;
    std::cerr << program << std::endl;
    return 1;
  }

  if (program.is_subcommand_used(help_cmd)) {
    std::cout << program << std::endl;
    return 0;
  }

  if (program.is_subcommand_used(version_cmd)) {
    std::cout << "Distributed File Grid Client version " << APP_VERSION << std::endl;
    return 0;
  }



  if (program.is_subcommand_used(delete_cmd)) {

    auto [server_ip, server_port] = resolve_server_target(delete_cmd, program);
    if (!validate_port(server_port)) return 1;
    std::string filename = delete_cmd.get<std::string>("filename");
    return file_delete(server_ip, server_port, filename);
  }

  if (program.is_subcommand_used(upload_cmd)) {

    auto [server_ip, server_port] = resolve_server_target(upload_cmd, program);
    if (!validate_port(server_port)) return 1;

    std::string filepath = upload_cmd.get<std::string>("filepath");
    std::string filename = upload_cmd.get<std::string>("filename");
    if (filename.empty()) {
      filename = extract_filename(filepath);
    }
    return file_upload(server_ip, server_port, filepath, filename);
  }

  if (program.is_subcommand_used(download_cmd)) {
    auto [server_ip, server_port] = resolve_server_target(download_cmd, program);
    if (!validate_port(server_port)) return 1;

    std::string filename = download_cmd.get<std::string>("filename");
    std::string output_path = download_cmd.get<std::string>("output_path");
    if (output_path.empty()) {
      if (download_cmd.is_used("-o")) {
        output_path = download_cmd.get<std::string>("-o");
      }
    }
    if (output_path.empty()) {
      output_path = filename;
    }
    return file_download(server_ip, server_port, filename, output_path);
  }

  if (program.is_subcommand_used(list_cmd)) {
    auto [server_ip, server_port] = resolve_server_target(list_cmd, program);
    if (!validate_port(server_port)) return 1;
    return file_list(server_ip, server_port);
  }

  if (program.is_subcommand_used(test_cmd)) {
    auto [server_ip, server_port] = resolve_server_target(test_cmd, program);
    if (!validate_port(server_port)) return 1;
    return run_client_test(server_ip, server_port);
  }

  // Top-level flag execution
  std::string server_ip = program.get<std::string>("--server_ip");
  int server_port = program.get<int>("--server_port");
  if (!validate_port(server_port)) return 1;

  if (program.get<bool>("--list")) {
    return file_list(server_ip, server_port);
  }

  if (program.get<bool>("--test")) {
    return run_client_test(server_ip, server_port);
  }

  if (auto upload_file = program.present<std::string>("--upload")) {
    std::string file_path = *upload_file;
    std::string file_name = extract_filename(file_path);
    return file_upload(server_ip, server_port, file_path, file_name);
  }

  std::string download_file;
  if (auto opt = program.present<std::string>("--download")) {
    download_file = *opt;
  } else if (auto opt = program.present<std::string>("--downlaod")) {
    download_file = *opt;
  }

  if (!download_file.empty()) {
    std::string output_path = download_file;
    if (program.is_used("-o")) {
      output_path = program.get<std::string>("-o");
    }
    return file_download(server_ip, server_port, download_file, output_path);
  }

  std::cerr << "Error: No command or operation flag specified.\n" << std::endl;
  std::cerr << program << std::endl;
  return 1;
}
