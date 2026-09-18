import re

with open('src/client/client.cpp', 'r') as f:
    content = f.read()

# I need to rewrite the file_download function.
# Let's write a new implementation in python that generates the C++ code and replaces it.
# Wait, I can just use a sed or python replace for the body of file_download.

new_file_download = """
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

  std::string req = "DOWNLOAD " + file_name + "\\n";
  send(sock, req.c_str(), req.length(), 0);

  std::string file_hash_from_server;

  auto read_line = [&](int fd) -> std::string {
    std::string line;
    char c;
    while (recv(fd, &c, 1, 0) == 1) {
      if (c == '\\n')
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
        std::cerr << "\\033[31mError: socket creation failed for chunk " << chunk.id << "\\033[0m" << std::endl;
        download_error = true;
        return;
      }
      sockaddr_in caddr;
      caddr.sin_family = AF_INET;
      caddr.sin_port = htons(chunk.port);
      if (inet_pton(AF_INET, chunk.ip.c_str(), &caddr.sin_addr) <= 0) {
        std::lock_guard<std::mutex> lock(mtx);
        std::cerr << "\\033[31mError: invalid IP for chunk " << chunk.id << "\\033[0m" << std::endl;
        download_error = true;
        close(csock);
        return;
      }
      if (connect(csock, (struct sockaddr *)&caddr, sizeof(caddr)) < 0) {
        std::lock_guard<std::mutex> lock(mtx);
        std::cerr << "\\033[31mError: connection failed for chunk " << chunk.id << "\\033[0m" << std::endl;
        download_error = true;
        close(csock);
        return;
      }

      std::string get_req = "GET_CHUNK " + std::to_string(chunk.id) + " " + file_name + "\\n";
      send(csock, get_req.c_str(), get_req.length(), 0);

      auto cread_line = [&](int fd) -> std::string {
        std::string line;
        char c;
        while (recv(fd, &c, 1, 0) == 1) {
          if (c == '\\n') break;
          line += c;
        }
        return line;
      };

      std::string resp = cread_line(csock);
      if (resp.rfind("CHUNK_SIZE ", 0) != 0) {
        std::lock_guard<std::mutex> lock(mtx);
        std::cerr << "\\033[31mError: server returned error for chunk " << chunk.id << ": " << resp << "\\033[0m" << std::endl;
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
        std::cerr << "\\033[31mError: incomplete data for chunk " << chunk.id << "\\033[0m" << std::endl;
        download_error = true;
        return;
      }

      std::string client_hash = calculate_checksum(buf);
      std::lock_guard<std::mutex> lock(mtx);
      if (client_hash != chunk.hash) {
        std::cerr << "\\033[31mError: Corrupted chunk received!\\n"
                  << "Order ID: " << chunk.id << "\\n"
                  << "Current Hash (Generated): " << client_hash << "\\n"
                  << "Expected Hash (from Head Server): " << chunk.hash
                  << "\\033[0m" << std::endl;
        download_error = true;
      } else {
        std::cout << "\\033[32mSuccessfully received and verified chunk " << chunk.id << "\\033[0m" << std::endl;
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
"""

start_idx = content.find('int file_download(std::string server_ip, int server_port, std::string file_name,')
end_idx = content.find('int file_upload(const std::string &server_ip, int server_port,')

if start_idx != -1 and end_idx != -1:
    content = content[:start_idx] + new_file_download + '\n' + content[end_idx:]
    with open('src/client/client.cpp', 'w') as f:
        f.write(content)
else:
    print("Could not find file_download or file_upload")
