#include "../include/heart_beat_signal.hpp"
#include "./redis_handler.hpp"
#include <fstream>
#include <filesystem>
#include <vector>
#include <string>
#include <iostream>
#include <sstream>
#include <map>
#include <algorithm>
#include "file_transfer.pb.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

namespace fs = std::filesystem;

struct ChunkLocation {
    int chunk_id;
    std::string server_ip;
    std::string file_path;
};

class FileReconstructor {
private:
    std::vector<ChunkLocation> get_chunk_locations_from_redis(const std::string& filename) {
        std::vector<ChunkLocation> locations;
        
        try {
            // Use the read_entry function to get chunk information
            std::stringstream request;
            request << filename;
            
            // Capture output from read_entry
            std::streambuf* orig = std::cout.rdbuf();
            std::ostringstream captured;
            std::cout.rdbuf(captured.rdbuf());
            
            read_entry(request.str());
            
            std::cout.rdbuf(orig);
            std::string output = captured.str();
            
            // Parse the output to extract chunk locations
            std::istringstream iss(output);
            std::string line;
            while (std::getline(iss, line)) {
                if (line.find("chunk:") != std::string::npos) {
                    // Parse line format: "chunk:X server=Y path=Z"
                    size_t chunk_pos = line.find("chunk:");
                    size_t server_pos = line.find("server=");
                    size_t path_pos = line.find("path=");
                    
                    if (chunk_pos != std::string::npos && server_pos != std::string::npos && path_pos != std::string::npos) {
                        ChunkLocation loc;
                        
                        // Extract chunk ID
                        std::string chunk_str = line.substr(chunk_pos + 6);
                        size_t space_pos = chunk_str.find(' ');
                        if (space_pos != std::string::npos) {
                            loc.chunk_id = std::stoi(chunk_str.substr(0, space_pos));
                        }
                        
                        // Extract server IP
                        std::string server_str = line.substr(server_pos + 7);
                        space_pos = server_str.find(' ');
                        if (space_pos != std::string::npos) {
                            loc.server_ip = server_str.substr(0, space_pos);
                        }
                        
                        // Extract file path
                        loc.file_path = line.substr(path_pos + 5);
                        
                        locations.push_back(loc);
                    }
                }
            }
        } catch (const std::exception& e) {
            std::cerr << "Error getting chunk locations: " << e.what() << std::endl;
        }
        
        return locations;
    }
    
    file_transfer::v1::FetchHashResponse get_chunk_hash(const ChunkLocation& location, const std::string& filename) {
        file_transfer::v1::FetchHashResponse resp;
        resp.set_success(false);
        std::string ip = location.server_ip;
        int port = 8080;
        size_t pos = ip.find(':');
        if (pos != std::string::npos) {
            port = std::stoi(ip.substr(pos + 1));
            ip = ip.substr(0, pos);
        }
        int transfer_port = port + 100;
        
        int sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) return resp;

        struct sockaddr_in serv_addr;
        serv_addr.sin_family = AF_INET;
        serv_addr.sin_port = htons(transfer_port);

        if (inet_pton(AF_INET, ip.c_str(), &serv_addr.sin_addr) <= 0) {
            close(sock);
            return resp;
        }

        if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
            close(sock);
            return resp;
        }

        file_transfer::v1::FetchHashRequest req;
        std::string unique_chunk_id = filename + "_chunk_" + std::to_string(location.chunk_id);
        req.set_chunk_id(unique_chunk_id);

        std::string serialized;
        req.SerializeToString(&serialized);
        uint32_t type = htonl(3);
        uint32_t len = htonl(serialized.size());
        
        send(sock, &type, sizeof(type), 0);
        send(sock, &len, sizeof(len), 0);
        
        size_t total_sent = 0;
        while(total_sent < serialized.size()) {
            ssize_t sent = send(sock, serialized.data() + total_sent, serialized.size() - total_sent, 0);
            if (sent < 0) {
               close(sock);
               return resp;
            }
            total_sent += sent;
        }
        
        uint32_t resp_len_net;
        if (recv(sock, &resp_len_net, sizeof(resp_len_net), MSG_WAITALL) != 4) {
             close(sock);
             return resp;
        }
        
        uint32_t resp_len = ntohl(resp_len_net);
        std::vector<char> resp_buf(resp_len);
        
        if (recv(sock, resp_buf.data(), resp_len, MSG_WAITALL) != resp_len) {
            close(sock);
            return resp;
        }
        
        resp.ParseFromArray(resp_buf.data(), resp_len);
        close(sock);
        return resp;
    }

    bool repair_chunk_on_server(const ChunkLocation& location, const std::string& filename, const std::vector<char>& chunk_data) {
        std::string ip = location.server_ip;
        int port = 8080;
        size_t pos = ip.find(':');
        if (pos != std::string::npos) {
            port = std::stoi(ip.substr(pos + 1));
            ip = ip.substr(0, pos);
        }
        int transfer_port = port + 100;
        
        int sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) return false;

        struct sockaddr_in serv_addr;
        serv_addr.sin_family = AF_INET;
        serv_addr.sin_port = htons(transfer_port);

        if (inet_pton(AF_INET, ip.c_str(), &serv_addr.sin_addr) <= 0) {
            close(sock);
            return false;
        }

        if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
            close(sock);
            return false;
        }

        file_transfer::v1::ChunkData msg;
        std::string unique_chunk_id = filename + "_chunk_" + std::to_string(location.chunk_id);
        msg.set_chunk_id(unique_chunk_id);
        msg.set_filename(filename);
        msg.set_data(chunk_data.data(), chunk_data.size());

        std::string serialized;
        msg.SerializeToString(&serialized);

        uint32_t type = htonl(1); // Same type as upload (1 = ChunkData)
        uint32_t len = htonl(serialized.size());
        
        send(sock, &type, sizeof(type), 0);
        send(sock, &len, sizeof(len), 0);
        
        size_t total_sent = 0;
        while(total_sent < serialized.size()) {
            ssize_t sent = send(sock, serialized.data() + total_sent, serialized.size() - total_sent, 0);
            if (sent < 0) {
               close(sock);
               return false;
            }
            total_sent += sent;
        }
        
        uint32_t resp_len_net;
        if (recv(sock, &resp_len_net, sizeof(resp_len_net), MSG_WAITALL) == 4) {
            uint32_t resp_len = ntohl(resp_len_net);
            std::vector<char> resp_buf(resp_len);
            if (recv(sock, resp_buf.data(), resp_len, MSG_WAITALL) == resp_len) {
                file_transfer::v1::ChunkResponse resp;
                if (resp.ParseFromArray(resp_buf.data(), resp_len) && resp.success()) {
                    close(sock);
                    return true;
                }
            }
        }
        close(sock);
        return false;
    }

    std::vector<char> read_chunk_from_server(const ChunkLocation& location, const std::string& filename) {
        std::vector<char> chunk_data;
        
        std::string ip = location.server_ip;
        int port = 8080;
        size_t pos = ip.find(':');
        if (pos != std::string::npos) {
            port = std::stoi(ip.substr(pos + 1));
            ip = ip.substr(0, pos);
        }
        int transfer_port = port + 100;
        
        int sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) return chunk_data;

        struct sockaddr_in serv_addr;
        serv_addr.sin_family = AF_INET;
        serv_addr.sin_port = htons(transfer_port);

        if (inet_pton(AF_INET, ip.c_str(), &serv_addr.sin_addr) <= 0) {
            close(sock);
            return chunk_data;
        }

        if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
            close(sock);
            return chunk_data;
        }

        file_transfer::v1::FetchChunkRequest req;
        std::string unique_chunk_id = filename + "_chunk_" + std::to_string(location.chunk_id);
        req.set_chunk_id(unique_chunk_id);

        std::string serialized;
        req.SerializeToString(&serialized);

        uint32_t type = htonl(2);
        uint32_t len = htonl(serialized.size());
        
        send(sock, &type, sizeof(type), 0);
        send(sock, &len, sizeof(len), 0);
        
        size_t total_sent = 0;
        while(total_sent < serialized.size()) {
            ssize_t sent = send(sock, serialized.data() + total_sent, serialized.size() - total_sent, 0);
            if (sent < 0) {
               close(sock);
               return chunk_data;
            }
            total_sent += sent;
        }
        
        uint32_t resp_len_net;
        if (recv(sock, &resp_len_net, sizeof(resp_len_net), MSG_WAITALL) != 4) {
             std::cerr << "Failed to receive fetch response length" << std::endl;
             close(sock);
             return chunk_data;
        }
        
        uint32_t resp_len = ntohl(resp_len_net);
        std::vector<char> resp_buf(resp_len);
        
        if (recv(sock, resp_buf.data(), resp_len, MSG_WAITALL) != resp_len) {
            close(sock);
            return chunk_data;
        }
        
        file_transfer::v1::FetchChunkResponse resp;
        if (!resp.ParseFromArray(resp_buf.data(), resp_len) || !resp.success()) {
            std::cerr << "Server rejected fetch: " << resp.error_message() << std::endl;
            close(sock);
            return chunk_data;
        }

        chunk_data.assign(resp.data().begin(), resp.data().end());
        close(sock);
        std::cout << "Read chunk " << location.chunk_id << " (" << chunk_data.size() << " bytes) from " << location.server_ip << std::endl;
        return chunk_data;
    }

public:
    bool reconstruct_file(const std::string& filename, const std::string& output_path) {
        std::cout << "Reconstructing file: " << filename << std::endl;
        
        // Get chunk locations from Redis
        auto chunk_locations = get_chunk_locations_from_redis(filename);
        if (chunk_locations.empty()) {
            std::cerr << "No chunks found for file: " << filename << std::endl;
            return false;
        }
        
        // Group chunks by chunk_id to find all replicas
        std::map<int, std::vector<ChunkLocation>> chunk_replicas;
        for (const auto& location : chunk_locations) {
            chunk_replicas[location.chunk_id].push_back(location);
        }
        
        std::cout << "Found " << chunk_replicas.size() << " unique chunks to reconstruct" << std::endl;
        
        // Create output file
        std::ofstream output_file(output_path, std::ios::binary);
        if (!output_file) {
            std::cerr << "Failed to create output file: " << output_path << std::endl;
            return false;
        }
        
        // Process each chunk in order (auto sorts by map int key)
        for (const auto& [chunk_id, replicas] : chunk_replicas) {
            std::map<std::string, int> hash_counts;
            std::map<std::string, std::vector<std::pair<ChunkLocation, float>>> hash_to_servers; 
            std::vector<ChunkLocation> failed_fetches;
            
            // 1. Ask all replicas for their chunk hash
            for (const auto& replica : replicas) {
                auto resp = get_chunk_hash(replica, filename);
                if (resp.success()) {
                    hash_counts[resp.hash()]++;
                    hash_to_servers[resp.hash()].push_back({replica, resp.load_score()});
                } else {
                    failed_fetches.push_back(replica);
                }
            }
            
            if (hash_counts.empty()) {
                std::cerr << "Could not verify any hashes for chunk " << chunk_id << " from any replica!" << std::endl;
                output_file.close();
                fs::remove(output_path);
                return false;
            }
            
            // 2. Find the majority hash (voting consensus)
            std::string majority_hash = "";
            int max_count = 0;
            for (const auto& [hash, count] : hash_counts) {
                if (count > max_count) {
                    max_count = count;
                    majority_hash = hash;
                }
            }
            
            // 3. Select the healthiest server with the majority hash
            auto& majority_servers = hash_to_servers[majority_hash];
            std::sort(majority_servers.begin(), majority_servers.end(), [](const auto& a, const auto& b) {
                return a.second < b.second; // ascending load score (lower load usage is better)
            });
            
            ChunkLocation best_server = majority_servers.front().first;
            std::cout << "Consensus for chunk " << chunk_id << ": " << majority_hash 
                      << " (Selected best server " << best_server.server_ip << " with load score " 
                      << majority_servers.front().second << ")" << std::endl;
            
            // 4. Fetch the verified chunk payload
            auto chunk_data = read_chunk_from_server(best_server, filename);
            if (chunk_data.empty()) {
                // Should technically failover to next best server, but simplified for now
                std::cerr << "Failed to fully stream chunk from selected master server" << std::endl;
                output_file.close();
                fs::remove(output_path);
                return false;
            }
            
            output_file.write(chunk_data.data(), chunk_data.size());
            
            // 5. Read Repair: Fix mismatched replicas out of sync with consensus memory
            for (const auto& [hash, servers] : hash_to_servers) {
                if (hash != majority_hash) {
                    for (const auto& [loc, score] : servers) {
                        std::cerr << "! Data Corruption Detected! Repairing Chunk " << chunk_id << " on " << loc.server_ip << std::endl;
                        repair_chunk_on_server(loc, filename, chunk_data);
                    }
                }
            }
        }
        
        output_file.close();
        std::cout << "File reconstructed successfully: " << output_path << std::endl;
        return true;
    }
    
    bool file_exists(const std::string& filename) {
        auto chunk_locations = get_chunk_locations_from_redis(filename);
        return !chunk_locations.empty();
    }
};

// Global file reconstructor instance
static FileReconstructor g_file_reconstructor;

extern "C" {
    int process_file_download(const char* filename, const char* output_path) {
        try {
            if (g_file_reconstructor.reconstruct_file(filename, output_path)) {
                return 0;
            }
            return -1;
        } catch (const std::exception& e) {
            std::cerr << "Error processing file download: " << e.what() << std::endl;
            return -1;
        }
    }
    
    int check_file_exists(const char* filename) {
        try {
            return g_file_reconstructor.file_exists(filename) ? 1 : 0;
        } catch (const std::exception& e) {
            std::cerr << "Error checking file existence: " << e.what() << std::endl;
            return -1;
        }
    }
}
