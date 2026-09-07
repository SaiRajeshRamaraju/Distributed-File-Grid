#include "dfg/sha256.hpp"
#include <argparse/argparse.hpp>
#include <arpa/inet.h>
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

  if (!file_hash_from_server.empty() && final_generated_hash != file_hash_from_server) {
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
    std::cerr << "Error: Failed to open local file for upload: " << file_path << std::endl;
    return 1;
  }

  std::streamsize file_size = in_file.tellg();
  in_file.seekg(0, std::ios::beg);

  std::cout << "Calculating SHA256 checksum for: " << file_path << " (" << file_size << " bytes)..." << std::endl;
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
    std::cerr << "Invalid address / Address not supported: " << server_ip << std::endl;
    close(sock);
    return 1;
  }

  if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
    std::cerr << "Connection to Head Server failed: " << server_ip << ":" << server_port << std::endl;
    close(sock);
    return 1;
  }

  // Send UPLOAD command: UPLOAD <filename> <file_size> <file_hash>\n
  std::string upload_cmd = "UPLOAD " + file_name + " " + std::to_string(file_size) + " " + file_hash + "\n";
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
    std::cerr << "Server did not respond with READY. Response: " << ready_resp << std::endl;
    close(sock);
    return 1;
  }

  std::cout << "Uploading file data to Head Server..." << std::endl;
  size_t total_sent = 0;
  while (in_file && total_sent < static_cast<size_t>(file_size)) {
    in_file.read(buffer.data(), buffer.size());
    std::streamsize bytes_read = in_file.gcount();
    if (bytes_read <= 0) break;

    size_t sent_chunk = 0;
    while (sent_chunk < static_cast<size_t>(bytes_read)) {
      ssize_t n = send(sock, buffer.data() + sent_chunk, bytes_read - sent_chunk, 0);
      if (n <= 0) {
        if (n < 0 && errno == EINTR) continue;
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

int main(int argc, char *argv[]) {
  // Check if invoked with legacy positional arguments:
  // dfg_client <server_ip> <server_port> <filename> [output_path]
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
      std::cerr << "Invalid port number: " << server_port << " (must be 1-65535)" << std::endl;
      return 1;
    }
    std::string download_file = argv[3];
    std::string output_path = (argc >= 5) ? argv[4] : download_file;
    return file_download(server_ip, server_port, download_file, output_path);
  }

  argparse::ArgumentParser program("dfg_client");
  program.add_argument("--server_ip", "--si")
      .required()
      .help("Head server IP address");
  program.add_argument("--server_port", "--sp")
      .required()
      .scan<'i', int>()
      .help("Head server port");
  program.add_argument("--upload").help("Filepath of the file to be uploaded");
  program.add_argument("--download").help("Filename of the download file");
  program.add_argument("--downlaod").help("Filename of the download file (alias for backward compatibility)");
  program.add_argument("-o", "--output")
      .help("Output path of the file to be stored");
  try {
    program.parse_args(argc, argv);
  } catch (const std::exception &e) {
    std::cerr << "Error while parsing arguments: " << e.what() << std::endl;
    return 1;
  }

  auto server_ip = program.get<std::string>("--server_ip");
  auto server_port = program.get<int>("--server_port");

  if (server_port <= 0 || server_port > 65535) {
    std::cerr << "Invalid port number: " << server_port << " (must be 1-65535)" << std::endl;
    return 1;
  }

  std::string download_file;
  if (auto opt = program.present<std::string>("--download")) {
    download_file = *opt;
  } else if (auto opt = program.present<std::string>("--downlaod")) {
    download_file = *opt;
  }

  if (auto upload_file = program.present<std::string>("--upload")) {
    std::string file_path = *upload_file;
    // Extract file basename as the stored filename
    std::string file_name = file_path;
    size_t last_slash = file_name.find_last_of("/\\");
    if (last_slash != std::string::npos) {
      file_name = file_name.substr(last_slash + 1);
    }
    return file_upload(server_ip, server_port, file_path, file_name);
  } else if (!download_file.empty()) {
    std::string output_path = download_file;
    if (auto opt = program.present<std::string>("-o")) {
      output_path = *opt;
    }
    return file_download(server_ip, server_port, download_file, output_path);
  } else {
    std::cerr << "Error: Either --upload <filepath> or --download <filename> must be specified." << std::endl;
    return 1;
  }

  return 0;
}
