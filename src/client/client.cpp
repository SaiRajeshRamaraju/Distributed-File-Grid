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

std::ofstream **file_to_chunks(std::string file_path) {
  std::ifstream fp(file_path, std::ios::binary);
  if (!fp.is_open()) {
    std::cerr << "Error: Failed to open file for chunking: " << file_path
              << std::endl;
    return nullptr;
  }

  fp.seekg(0, std::ios::end);
  std::streamsize file_size = fp.tellg();
  fp.seekg(0, std::ios::beg);

  if (file_size < 0) {
    std::cerr << "Error: Could not determine file size for: " << file_path
              << std::endl;
    return nullptr;
  }

  const size_t CHUNK_SIZE = DEFAULT_CHUNK_SIZE;
  size_t num_chunks = (file_size == 0)
                          ? 0
                          : static_cast<size_t>((file_size + CHUNK_SIZE - 1) / CHUNK_SIZE);

  std::ofstream **chunks = new std::ofstream *[num_chunks + 1];
  chunks[num_chunks] = nullptr;

  if (num_chunks == 0) {
    return chunks;
  }

  std::vector<char> buffer(64 * 1024);
  size_t bytes_read_total = 0;

  for (size_t i = 0; i < num_chunks; ++i) {
    std::string chunk_name = file_path + "_chunk_" + std::to_string(i);
    chunks[i] = new std::ofstream(chunk_name, std::ios::binary | std::ios::trunc);
    if (!chunks[i]->is_open()) {
      std::cerr << "Error: Failed to create chunk file: " << chunk_name
                << std::endl;
      for (size_t j = 0; j <= i; ++j) {
        if (chunks[j]) {
          if (chunks[j]->is_open()) {
            chunks[j]->close();
          }
          delete chunks[j];
        }
      }
      delete[] chunks;
      return nullptr;
    }

    size_t current_chunk_size =
        std::min(CHUNK_SIZE, static_cast<size_t>(file_size - bytes_read_total));
    size_t written_for_chunk = 0;

    while (written_for_chunk < current_chunk_size && fp) {
      size_t to_read =
          std::min(buffer.size(), current_chunk_size - written_for_chunk);
      fp.read(buffer.data(), to_read);
      std::streamsize bytes_read = fp.gcount();
      if (bytes_read <= 0) {
        break;
      }
      chunks[i]->write(buffer.data(), bytes_read);
      written_for_chunk += bytes_read;
    }

    bytes_read_total += written_for_chunk;
    chunks[i]->flush();
  }

  return chunks;
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

  std::ofstream out_file(output_path, std::ios::binary);
  if (!out_file) {
    std::cerr << "Failed to open output file" << std::endl;
    return 1;
  }

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

  dfg::hash::SHA256 full_file_hash;

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
      std::cout << "Received file hash from server: " << file_hash_from_server
                << std::endl;
    } else if (line.rfind("CHUNK ", 0) == 0) {
      std::istringstream iss(line.substr(6));
      int order_id;
      size_t chunk_size;
      std::string server_hash;
      if (!(iss >> order_id >> chunk_size >> server_hash)) {
        std::cerr << "Invalid chunk header: " << line << std::endl;
        return 1;
      }

      std::cout << "Receiving chunk " << order_id << " size " << chunk_size
                << std::endl;
      std::vector<char> buffer(chunk_size);
      size_t total_received = 0;
      while (total_received < chunk_size) {
        ssize_t n = recv(sock, buffer.data() + total_received,
                         chunk_size - total_received, 0);
        if (n <= 0) {
          std::cerr << "Failed to receive chunk data" << std::endl;
          return 1;
        }
        total_received += n;
      }

      std::string client_hash = calculate_checksum(buffer);
      if (client_hash != server_hash) {
        std::cerr << "Error: Corrupted chunk received!\n"
                  << "Order ID: " << order_id << "\n"
                  << "Current Hash (Generated): " << client_hash << "\n"
                  << "Expected Hash (from Head Server): " << server_hash
                  << std::endl;

        out_file.close();
        remove(output_path.c_str());
        return 1;
      }

      full_file_hash.update(buffer);

      out_file.write(buffer.data(), buffer.size());

    } else if (line == "EOF") {
      break;
    } else {
      std::cerr << "Unknown response: " << line << std::endl;
    }
  }

  out_file.close();
  close(sock);

  std::string final_generated_hash = full_file_hash.digest();

  if (!file_hash_from_server.empty() &&
      final_generated_hash != file_hash_from_server) {
    std::cerr << "Error: Full aggregated file hash mismatch!\n"
              << "Hash from server: " << file_hash_from_server << "\n"
              << "Generated hash:   " << final_generated_hash << std::endl;
    remove(output_path.c_str());
    return 1;
  }

  std::cout << "File downloaded and verified successfully: " << output_path
            << std::endl;
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

  // Send UPLOAD command: UPLOAD <filename> <file_size> <file_hash>\n
  std::string upload_cmd = "UPLOAD " + file_name + " " +
                           std::to_string(file_size) + " " + file_hash + "\n";
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

  std::cout << "Uploading file data to Head Server..." << std::endl;
  size_t total_sent = 0;
  while (in_file && total_sent < static_cast<size_t>(file_size)) {
    in_file.read(buffer.data(), buffer.size());
    std::streamsize bytes_read = in_file.gcount();
    if (bytes_read <= 0)
      break;

    size_t sent_chunk = 0;
    while (sent_chunk < static_cast<size_t>(bytes_read)) {
      ssize_t n =
          send(sock, buffer.data() + sent_chunk, bytes_read - sent_chunk, 0);
      if (n <= 0) {
        if (n < 0 && errno == EINTR)
          continue;
        std::cerr << "Failed to send file stream data to server" << std::endl;
        close(sock);
        return 1;
      }
      sent_chunk += n;
    }
    total_sent += bytes_read;
  }
  in_file.close();

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

static void print_usage() {
  std::cout << R"(
╔═══════════════════════════════════════════════════╗
║       Distributed File Grid — Client CLI          ║
╚═══════════════════════════════════════════════════╝

Usage:
  client <command> [arguments] [options]
  client [flags]
  client <server_ip> <server_port> <filename> [output_path]

Commands:
  upload <filepath> [remote_name]       Upload a file to distributed storage
  download <remote_name> [output_path]  Download a file from distributed storage
  list                                  List all files in storage
  test                                  Run upload/download verification test

Options / Flags:
  --server_ip, --si, -s <IP>   Head server IP address (default: 127.0.0.1)
  --server_port, --sp, -p <P>  Head server port (default: 9669)
  --upload <filepath>          Filepath of the file to be uploaded
  --download <filename>        Filename of the file to download
  -o, --output <path>          Output path of the downloaded file
  -h, --help                   Show this help message
  -v, --version                Show version information

Examples:
  client upload /path/to/file.txt myfile.txt
  client download myfile.txt /path/to/output.txt
  client list
  client test
  client --server_ip 127.0.0.1 --server_port 9669 --upload /path/to/file.txt
  client --server_ip 127.0.0.1 --server_port 9669 --download myfile.txt -o /path/to/out.txt
  client 127.0.0.1 9669 myfile.txt /path/to/out.txt
)" << std::endl;
}

int main(int argc, char *argv[]) {
  if (argc < 2) {
    print_usage();
    return 1;
  }

  std::string first_arg = argv[1];

  if (first_arg == "-h" || first_arg == "--help" || first_arg == "help") {
    print_usage();
    return 0;
  }

  if (first_arg == "-v" || first_arg == "--version" || first_arg == "version") {
    std::cout << "Distributed File Grid Client version " << APP_VERSION << std::endl;
    return 0;
  }

  // Subcommands: upload, download, list, test
  if (first_arg == "upload" || first_arg == "download" || first_arg == "list" || first_arg == "test") {
    std::string server_ip = get_default_server_ip();
    int server_port = get_default_server_port();
    std::vector<std::string> positional;

    for (int i = 2; i < argc; ++i) {
      std::string arg = argv[i];
      if ((arg == "--server_ip" || arg == "--si" || arg == "-s") && i + 1 < argc) {
        server_ip = argv[++i];
      } else if ((arg == "--server_port" || arg == "--sp" || arg == "-p") && i + 1 < argc) {
        try {
          server_port = std::stoi(argv[++i]);
        } catch (...) {
          std::cerr << "Invalid port number: " << argv[i] << std::endl;
          return 1;
        }
      } else if (arg[0] != '-') {
        positional.push_back(arg);
      }
    }

    if (server_port <= 0 || server_port > 65535) {
      std::cerr << "Invalid port number: " << server_port << " (must be 1-65535)" << std::endl;
      return 1;
    }

    if (first_arg == "upload") {
      if (positional.empty()) {
        std::cerr << "Usage: client upload <filepath> [remote_name] [--server_ip IP] [--server_port PORT]" << std::endl;
        return 1;
      }
      std::string filepath = positional[0];
      std::string filename = (positional.size() > 1) ? positional[1] : extract_filename(filepath);
      return file_upload(server_ip, server_port, filepath, filename);
    } else if (first_arg == "download") {
      if (positional.empty()) {
        std::cerr << "Usage: client download <filename> [output_path] [--server_ip IP] [--server_port PORT]" << std::endl;
        return 1;
      }
      std::string filename = positional[0];
      std::string output_path = (positional.size() > 1) ? positional[1] : filename;
      return file_download(server_ip, server_port, filename, output_path);
    } else if (first_arg == "list") {
      return file_list(server_ip, server_port);
    } else if (first_arg == "test") {
      return run_client_test(server_ip, server_port);
    }
  }

  // Legacy positional arguments:
  // client <server_ip> <server_port> <filename> [output_path]
  if (argc >= 4 && argv[1][0] != '-') {
    std::string server_ip = argv[1];
    int server_port = 0;
    try {
      server_port = std::stoi(argv[2]);
    } catch (...) {
      std::cerr << "Invalid port: " << argv[2] << std::endl;
      return 1;
    }
    if (server_port <= 0 || server_port > 65535) {
      std::cerr << "Invalid port number: " << server_port
                << " (must be 1-65535)" << std::endl;
      return 1;
    }
    std::string download_file = argv[3];
    std::string output_path = (argc >= 5) ? argv[4] : download_file;
    return file_download(server_ip, server_port, download_file, output_path);
  }

  // Fallback to argparse flags:
  // e.g. client --server_ip 127.0.0.1 --server_port 9669 --upload ...
  argparse::ArgumentParser program("client");
  program.add_argument("--server_ip", "--si")
      .default_value(get_default_server_ip())
      .help("Head server IP address");
  program.add_argument("--server_port", "--sp")
      .default_value(get_default_server_port())
      .scan<'i', int>()
      .help("Head server port");
  program.add_argument("--upload").help("Filepath of the file to be uploaded");
  program.add_argument("--download").help("Filename of the download file");
  program.add_argument("--downlaod")
      .help("Filename of the download file (alias for backward compatibility)");
  program.add_argument("-o", "--output")
      .help("Output path of the file to be stored");
  program.add_argument("--list")
      .default_value(false)
      .implicit_value(true)
      .help("List stored files");

  try {
    program.parse_args(argc, argv);
  } catch (const std::exception &e) {
    std::cerr << "Error while parsing arguments: " << e.what() << std::endl;
    print_usage();
    return 1;
  }

  auto server_ip = program.get<std::string>("--server_ip");
  auto server_port = program.get<int>("--server_port");

  if (server_port <= 0 || server_port > 65535) {
    std::cerr << "Invalid port number: " << server_port << " (must be 1-65535)"
              << std::endl;
    return 1;
  }

  if (program.get<bool>("--list")) {
    return file_list(server_ip, server_port);
  }

  std::string download_file;
  if (auto opt = program.present<std::string>("--download")) {
    download_file = *opt;
  } else if (auto opt = program.present<std::string>("--downlaod")) {
    download_file = *opt;
  }

  if (auto upload_file = program.present<std::string>("--upload")) {
    std::string file_path = *upload_file;
    std::string file_name = extract_filename(file_path);
    return file_upload(server_ip, server_port, file_path, file_name);
  } else if (!download_file.empty()) {
    std::string output_path = download_file;
    if (auto opt = program.present<std::string>("-o")) {
      output_path = *opt;
    }
    return file_download(server_ip, server_port, download_file, output_path);
  } else {
    std::cerr << "Error: Either a command (upload, download, list, test) or flags (--upload, --download) must be specified."
              << std::endl;
    return 1;
  }

  return 0;
}
