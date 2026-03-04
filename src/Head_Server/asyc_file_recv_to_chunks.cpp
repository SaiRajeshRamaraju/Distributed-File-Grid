#include "../include/heart_beat_signal.hpp"
#include "./redis_handler.hpp"
#include <fstream>
#include <filesystem>
#include <vector>
#include <string>
#include <iostream>
#include <sstream>
#include <thread>
#include <future>
#include <random>
#include <algorithm>
#include "file_transfer.pb.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

namespace fs = std::filesystem;

const size_t CHUNK_SIZE = 64 * 1024 * 1024; // 64MB chunks
const int DEFAULT_REPLICATION_FACTOR = 3;

struct ChunkInfo {
    int chunk_id;
    std::string server_ip;
    std::string file_path;
    size_t size;
    std::string checksum;
};

class FileChunker {
private:
    std::vector<std::string> cluster_servers = {
        "127.0.0.1:8080",
        "127.0.0.1:8081", 
        "127.0.0.1:8082"
    };
    
    std::string calculate_checksum(const std::vector<char>& data) {
        // Simple checksum - in production use SHA256
        size_t hash = 0;
        for (char c : data) {
            hash = hash * 31 + static_cast<size_t>(c);
        }
        std::stringstream ss;
        ss << std::hex << hash;
        return ss.str();
    }
    
    std::vector<std::string> select_servers_for_chunk(int replication_factor) {
        std::vector<std::string> selected;
        std::random_device rd;
        std::mt19937 gen(rd());
        
        auto servers_copy = cluster_servers;
        std::shuffle(servers_copy.begin(), servers_copy.end(), gen);
        
        int count = std::min(replication_factor, static_cast<int>(servers_copy.size()));
        for (int i = 0; i < count; i++) {
            selected.push_back(servers_copy[i]);
        }
        return selected;
    }
    
    bool send_chunk_to_server(const std::string& server, int chunk_id, 
                             const std::vector<char>& chunk_data, const std::string& filename) {
        
        std::string ip = server;
        int port = 8080;
        size_t pos = server.find(':');
        if (pos != std::string::npos) {
            ip = server.substr(0, pos);
            port = std::stoi(server.substr(pos + 1));
        }
        int transfer_port = port + 100;
        
        int sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) {
            std::cerr << "Socket creation error" << std::endl;
            return false;
        }

        struct sockaddr_in serv_addr;
        serv_addr.sin_family = AF_INET;
        serv_addr.sin_port = htons(transfer_port);

        if (inet_pton(AF_INET, ip.c_str(), &serv_addr.sin_addr) <= 0) {
            std::cerr << "Invalid address for file transfer: " << ip << std::endl;
            close(sock);
            return false;
        }

        if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
            std::cerr << "Connection failed to transfoer port " << ip << ":" << transfer_port << std::endl;
            close(sock);
            return false;
        }

        file_transfer::v1::ChunkData msg;
        // Make chunk_id unique incorporating filename so fetching is accurate
        std::string unique_chunk_id = filename + "_chunk_" + std::to_string(chunk_id);
        msg.set_chunk_id(unique_chunk_id);
        msg.set_filename(filename);
        msg.set_data(chunk_data.data(), chunk_data.size());

        std::string serialized;
        msg.SerializeToString(&serialized);

        uint32_t type = htonl(1);
        uint32_t len = htonl(serialized.size());
        
        send(sock, &type, sizeof(type), 0);
        send(sock, &len, sizeof(len), 0);
        
        size_t total_sent = 0;
        while(total_sent < serialized.size()) {
            ssize_t sent = send(sock, serialized.data() + total_sent, serialized.size() - total_sent, 0);
            if (sent < 0) {
               std::cerr << "Failed to send chunk data" << std::endl;
               close(sock);
               return false;
            }
            total_sent += sent;
        }
        
        uint32_t resp_len_net;
        if (recv(sock, &resp_len_net, sizeof(resp_len_net), MSG_WAITALL) != 4) {
             std::cerr << "Failed to receive response length" << std::endl;
             close(sock);
             return false;
        }
        
        uint32_t resp_len = ntohl(resp_len_net);
        std::vector<char> resp_buf(resp_len);
        
        if (recv(sock, resp_buf.data(), resp_len, MSG_WAITALL) != resp_len) {
            close(sock);
            return false;
        }
        
        file_transfer::v1::ChunkResponse resp;
        if (!resp.ParseFromArray(resp_buf.data(), resp_len) || !resp.success()) {
            std::cerr << "Server rejected chunk: " << resp.error_message() << std::endl;
            close(sock);
            return false;
        }

        close(sock);
        std::cout << "Stored chunk " << chunk_id << " on server " << server << std::endl;
        return true;
    }

public:
    std::vector<ChunkInfo> split_and_store_file(const std::string& filepath, const std::string& filename) {
        std::vector<ChunkInfo> chunks;
        
        std::ifstream file(filepath, std::ios::binary);
        if (!file) {
            std::cerr << "Failed to open file: " << filepath << std::endl;
            return chunks;
        }
        
        // Get file size
        file.seekg(0, std::ios::end);
        size_t file_size = file.tellg();
        file.seekg(0, std::ios::beg);
        
        std::cout << "Splitting file " << filename << " (" << file_size << " bytes) into chunks..." << std::endl;
        
        int chunk_id = 0;
        size_t bytes_read = 0;
        
        while (bytes_read < file_size) {
            size_t current_chunk_size = std::min(CHUNK_SIZE, file_size - bytes_read);
            std::vector<char> chunk_data(current_chunk_size);
            
            file.read(chunk_data.data(), current_chunk_size);
            bytes_read += current_chunk_size;
            
            std::string checksum = calculate_checksum(chunk_data);
            auto selected_servers = select_servers_for_chunk(DEFAULT_REPLICATION_FACTOR);
            
            // Store chunk on selected servers
            for (const auto& server : selected_servers) {
                if (send_chunk_to_server(server, chunk_id, chunk_data, filename)) {
                    ChunkInfo chunk_info;
                    chunk_info.chunk_id = chunk_id;
                    chunk_info.server_ip = server;
                    chunk_info.file_path = "/tmp/chunks/" + server + "_" + filename + "_chunk_" + std::to_string(chunk_id);
                    chunk_info.size = current_chunk_size;
                    chunk_info.checksum = checksum;
                    chunks.push_back(chunk_info);
                }
            }
            
            chunk_id++;
        }
        
        file.close();
        std::cout << "File split into " << chunk_id << " chunks with " << DEFAULT_REPLICATION_FACTOR << "x replication" << std::endl;
        return chunks;
    }
    
    void store_metadata_in_redis(const std::string& filename, const std::vector<ChunkInfo>& chunks) {
        std::stringstream request;
        request << filename << "\n";
        request << "TTL=3600\n"; // 1 hour TTL
        
        for (const auto& chunk : chunks) {
            request << chunk.chunk_id << " " << chunk.server_ip << " " << chunk.file_path << "\n";
        }
        
        create_entry(request.str());
        std::cout << "Metadata stored in Redis for file: " << filename << std::endl;
    }
};

// Global file chunker instance
static FileChunker g_file_chunker;

extern "C" {
    int process_file_upload(const char* filepath, const char* filename) {
        try {
            auto chunks = g_file_chunker.split_and_store_file(filepath, filename);
            if (chunks.empty()) {
                return -1;
            }
            
            g_file_chunker.store_metadata_in_redis(filename, chunks);
            return 0;
        } catch (const std::exception& e) {
            std::cerr << "Error processing file upload: " << e.what() << std::endl;
            return -1;
        }
    }
}