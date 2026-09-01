#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <sstream>
#include <iomanip>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <stdexcept>
#include "dfg/sha256.hpp"

std::string calculate_checksum(const std::vector<char> &data) {
    return dfg::hash::sha256(data);
}

int main(int argc, char* argv[]) {
    if (argc != 5) {
        std::cerr << "Usage: " << argv[0] << " <head_ip> <head_port> <filename> <output_path>" << std::endl;
        return 1;
    }

    std::string ip = argv[1];
    int port = std::stoi(argv[2]);
    std::string filename = argv[3];
    std::string output_path = argv[4];

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        std::cerr << "Socket creation error" << std::endl;
        return 1;
    }

    sockaddr_in serv_addr;
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);
    if (inet_pton(AF_INET, ip.c_str(), &serv_addr.sin_addr) <= 0) {
        std::cerr << "Invalid address/ Address not supported" << std::endl;
        return 1;
    }

    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "Connection Failed" << std::endl;
        return 1;
    }

    std::string req = "DOWNLOAD " + filename + "\n";
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
            if (c == '\n') break;
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
            std::cout << "Received file hash from server: " << file_hash_from_server << std::endl;
        } else if (line.rfind("CHUNK ", 0) == 0) {
            std::istringstream iss(line.substr(6));
            int order_id;
            size_t chunk_size;
            std::string server_hash;
            if (!(iss >> order_id >> chunk_size >> server_hash)) {
                std::cerr << "Invalid chunk header: " << line << std::endl;
                return 1;
            }
            
            std::cout << "Receiving chunk " << order_id << " size " << chunk_size << std::endl;
            std::vector<char> buffer(chunk_size);
            size_t total_received = 0;
            while (total_received < chunk_size) {
                ssize_t n = recv(sock, buffer.data() + total_received, chunk_size - total_received, 0);
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
                          << "Expected Hash (from Head Server): " << server_hash << std::endl;
                
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
    
    if (final_generated_hash != file_hash_from_server) {
        std::cerr << "Error: Full aggregated file hash mismatch!\n"
                  << "Hash from server: " << file_hash_from_server << "\n"
                  << "Generated hash:   " << final_generated_hash << std::endl;
        remove(output_path.c_str());
        return 1;
    }

    std::cout << "File downloaded and verified successfully: " << output_path << std::endl;
    return 0;
}
