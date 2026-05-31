// ─────────────────── File Download / Reconstruction ───────────────────
// Fetches chunks from cluster servers, verifies via hash voting,
// performs read repairs, and reassembles the original file.
// Uses shared net_utils and thread_pool.

#include <dfg/heart_beat_signal.hpp>
#include <dfg/config_loader.hpp>
#include <dfg/net_utils.hpp>
#include <dfg/thread_pool.hpp>
#include "redis_handler.hpp"
#include "file_transfer.pb.h"

#include <fstream>
#include <filesystem>
#include <vector>
#include <string>
#include <iostream>
#include <sstream>
#include <map>
#include <algorithm>
#include <future>
#include <functional>
#include <atomic>
#include <fcntl.h>

namespace fs = std::filesystem;

static constexpr int MAX_CONCURRENT_FETCHES = 6;

struct ChunkLocation {
    int chunk_id;
    std::string server_ip;
    std::string file_path;
};

static dfg::ThreadPool& fetch_pool() {
    static dfg::ThreadPool pool(MAX_CONCURRENT_FETCHES);
    return pool;
}

class FileReconstructor {
private:
    std::vector<ChunkLocation> get_chunk_locations_from_metadata(const std::string& filename) {
        std::vector<ChunkLocation> locations;
        
        try {
            std::stringstream request;
            request << filename;
            
            // Capture output from read_entry
            std::streambuf* orig = std::cout.rdbuf();
            std::ostringstream captured;
            std::cout.rdbuf(captured.rdbuf());
            
            read_entry(request.str());
            
            std::cout.rdbuf(orig);
            std::string output = captured.str();
            
            std::istringstream iss(output);
            std::string line;
            while (std::getline(iss, line)) {
                if (line.find("chunk:") != std::string::npos) {
                    size_t chunk_pos = line.find("chunk:");
                    size_t server_pos = line.find("server=");
                    size_t path_pos = line.find("path=");
                    
                    if (chunk_pos != std::string::npos && server_pos != std::string::npos && path_pos != std::string::npos) {
                        ChunkLocation loc;
                        
                        std::string chunk_str = line.substr(chunk_pos + 6);
                        size_t space_pos = chunk_str.find(' ');
                        if (space_pos != std::string::npos) {
                            loc.chunk_id = std::stoi(chunk_str.substr(0, space_pos));
                        }
                        
                        std::string server_str = line.substr(server_pos + 7);
                        space_pos = server_str.find(' ');
                        if (space_pos != std::string::npos) {
                            loc.server_ip = server_str.substr(0, space_pos);
                        }
                        
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

        std::string ip;
        int port;
        dfg::net::parse_address(location.server_ip, ip, port);
        int transfer_port = port + 100;
        
        int sock = dfg::net::connect_with_timeout(ip, transfer_port);
        if (sock < 0) return resp;

        file_transfer::v1::FetchHashRequest req;
        std::string unique_chunk_id = filename + "_chunk_" + std::to_string(location.chunk_id);
        req.set_chunk_id(unique_chunk_id);

        std::string serialized;
        req.SerializeToString(&serialized);
        uint32_t type = htonl(3);
        uint32_t len = htonl(serialized.size());
        
        if (!dfg::net::send_all(sock, &type, sizeof(type)) ||
            !dfg::net::send_all(sock, &len, sizeof(len)) ||
            !dfg::net::send_all(sock, serialized.data(), serialized.size())) {
            ::close(sock);
            return resp;
        }
        
        uint32_t resp_len_net;
        if (!dfg::net::recv_all(sock, &resp_len_net, sizeof(resp_len_net))) {
            ::close(sock);
            return resp;
        }
        
        uint32_t resp_len = ntohl(resp_len_net);
        std::vector<char> resp_buf(resp_len);
        
        if (!dfg::net::recv_all(sock, resp_buf.data(), resp_len)) {
            ::close(sock);
            return resp;
        }
        
        resp.ParseFromArray(resp_buf.data(), resp_len);
        ::close(sock);
        return resp;
    }

    bool repair_chunk_on_server(const ChunkLocation& location, const std::string& filename, const std::vector<char>& chunk_data) {
        std::string ip;
        int port;
        dfg::net::parse_address(location.server_ip, ip, port);
        int transfer_port = port + 100;
        
        int sock = dfg::net::connect_with_timeout(ip, transfer_port);
        if (sock < 0) return false;

        file_transfer::v1::ChunkData msg;
        std::string unique_chunk_id = filename + "_chunk_" + std::to_string(location.chunk_id);
        msg.set_chunk_id(unique_chunk_id);
        msg.set_filename(filename);
        msg.set_data(chunk_data.data(), chunk_data.size());

        std::string serialized;
        msg.SerializeToString(&serialized);

        uint32_t type = htonl(1);
        uint32_t len = htonl(serialized.size());
        
        if (!dfg::net::send_all(sock, &type, sizeof(type)) ||
            !dfg::net::send_all(sock, &len, sizeof(len)) ||
            !dfg::net::send_all(sock, serialized.data(), serialized.size())) {
            ::close(sock);
            return false;
        }
        
        uint32_t resp_len_net;
        if (dfg::net::recv_all(sock, &resp_len_net, sizeof(resp_len_net))) {
            uint32_t resp_len = ntohl(resp_len_net);
            std::vector<char> resp_buf(resp_len);
            if (dfg::net::recv_all(sock, resp_buf.data(), resp_len)) {
                file_transfer::v1::ChunkResponse resp;
                if (resp.ParseFromArray(resp_buf.data(), resp_len) && resp.success()) {
                    ::close(sock);
                    return true;
                }
            }
        }
        ::close(sock);
        return false;
    }

    std::vector<char> read_chunk_from_server(const ChunkLocation& location, const std::string& filename) {
        std::vector<char> chunk_data;
        
        std::string ip;
        int port;
        dfg::net::parse_address(location.server_ip, ip, port);
        int transfer_port = port + 100;
        
        int sock = dfg::net::connect_with_timeout(ip, transfer_port);
        if (sock < 0) return chunk_data;

        file_transfer::v1::FetchChunkRequest req;
        std::string unique_chunk_id = filename + "_chunk_" + std::to_string(location.chunk_id);
        req.set_chunk_id(unique_chunk_id);

        std::string serialized;
        req.SerializeToString(&serialized);

        uint32_t type = htonl(2);
        uint32_t len = htonl(serialized.size());
        
        if (!dfg::net::send_all(sock, &type, sizeof(type)) ||
            !dfg::net::send_all(sock, &len, sizeof(len)) ||
            !dfg::net::send_all(sock, serialized.data(), serialized.size())) {
            ::close(sock);
            return chunk_data;
        }
        
        uint32_t resp_len_net;
        if (!dfg::net::recv_all(sock, &resp_len_net, sizeof(resp_len_net))) {
            ::close(sock);
            return chunk_data;
        }
        
        uint32_t resp_len = ntohl(resp_len_net);
        std::vector<char> resp_buf(resp_len);
        
        if (!dfg::net::recv_all(sock, resp_buf.data(), resp_len)) {
            ::close(sock);
            return chunk_data;
        }
        
        file_transfer::v1::FetchChunkResponse resp;
        if (!resp.ParseFromArray(resp_buf.data(), resp_len) || !resp.success()) {
            std::cerr << "Server rejected fetch: " << resp.error_message() << std::endl;
            ::close(sock);
            return chunk_data;
        }

        chunk_data.assign(resp.data().begin(), resp.data().end());
        ::close(sock);
        std::cout << "Read chunk " << location.chunk_id << " (" << chunk_data.size() << " bytes) from " << location.server_ip << std::endl;
        return chunk_data;
    }

public:
    bool reconstruct_file(const std::string& filename, const std::string& output_path) {
        std::cout << "Reconstructing file: " << filename << std::endl;
        
        auto chunk_locations = get_chunk_locations_from_metadata(filename);
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
        
        // Phase 1: Fetch all hashes concurrently
        struct HashResult {
            int chunk_id;
            ChunkLocation location;
            file_transfer::v1::FetchHashResponse response;
        };
        
        std::vector<std::future<HashResult>> hash_futures;
        
        for (const auto& [chunk_id, replicas] : chunk_replicas) {
            for (const auto& replica : replicas) {
                auto cid = chunk_id;
                auto loc = replica;
                auto fname = filename;
                
                hash_futures.push_back(fetch_pool().submit([this, cid, loc, fname]() -> HashResult {
                    auto resp = get_chunk_hash(loc, fname);
                    return HashResult{cid, loc, resp};
                }));
            }
        }
        
        std::cout << "Fetching " << hash_futures.size() << " chunk hashes concurrently..." << std::endl;
        
        struct ChunkConsensus {
            std::map<std::string, int> hash_counts;
            std::map<std::string, std::vector<std::pair<ChunkLocation, float>>> hash_to_servers;
            std::vector<ChunkLocation> failed_fetches;
        };
        std::map<int, ChunkConsensus> consensus_map;
        
        for (auto& fut : hash_futures) {
            auto result = fut.get();
            auto& consensus = consensus_map[result.chunk_id];
            if (result.response.success()) {
                consensus.hash_counts[result.response.hash()]++;
                consensus.hash_to_servers[result.response.hash()].push_back(
                    {result.location, result.response.load_score()});
            } else {
                consensus.failed_fetches.push_back(result.location);
            }
        }
        
        // Phase 2: Select best servers and fetch
        struct ChunkFetchPlan {
            int chunk_id;
            ChunkLocation best_server;
            std::string majority_hash;
        };
        
        std::vector<ChunkFetchPlan> fetch_plans;
        
        for (const auto& [chunk_id, consensus] : consensus_map) {
            if (consensus.hash_counts.empty()) {
                std::cerr << "Could not verify any hashes for chunk " << chunk_id << std::endl;
                return false;
            }
            
            std::string majority_hash;
            int max_count = 0;
            for (const auto& [hash, count] : consensus.hash_counts) {
                if (count > max_count) {
                    max_count = count;
                    majority_hash = hash;
                }
            }
            
            auto it = consensus.hash_to_servers.find(majority_hash);
            if (it == consensus.hash_to_servers.end() || it->second.empty()) {
                std::cerr << "No servers available for chunk " << chunk_id << std::endl;
                return false;
            }
            
            auto majority_servers = it->second;
            std::sort(majority_servers.begin(), majority_servers.end(), [](const auto& a, const auto& b) {
                return a.second < b.second;
            });
            
            fetch_plans.push_back(ChunkFetchPlan{chunk_id, majority_servers.front().first, majority_hash});
        }
        
        // Fire all fetches
        struct ChunkFetchResult {
            int chunk_id;
            std::vector<char> data;
            std::string majority_hash;
        };
        
        std::vector<std::future<ChunkFetchResult>> fetch_futures;
        
        for (const auto& plan : fetch_plans) {
            auto fname = filename;
            auto p = plan;
            fetch_futures.push_back(fetch_pool().submit([this, fname, p]() -> ChunkFetchResult {
                auto data = read_chunk_from_server(p.best_server, fname);
                return ChunkFetchResult{p.chunk_id, std::move(data), p.majority_hash};
            }));
        }
        
        std::map<int, ChunkFetchResult> fetched_chunks;
        for (auto& fut : fetch_futures) {
            auto result = fut.get();
            if (result.data.empty()) {
                std::cerr << "Failed to fetch chunk " << result.chunk_id << std::endl;
                return false;
            }
            fetched_chunks[result.chunk_id] = std::move(result);
        }
        
        // Phase 3: Write output
        int out_fd = ::open(output_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
        if (out_fd < 0) {
            std::cerr << "Failed to create output file: " << output_path << std::endl;
            return false;
        }
        
        for (const auto& [chunk_id, result] : fetched_chunks) {
            size_t written = 0;
            while (written < result.data.size()) {
                ssize_t n = ::write(out_fd, result.data.data() + written, result.data.size() - written);
                if (n < 0) {
                    if (errno == EINTR) continue;
                    ::close(out_fd);
                    fs::remove(output_path);
                    return false;
                }
                written += static_cast<size_t>(n);
            }
        }
        
        ::fdatasync(out_fd);
        ::close(out_fd);
        
        // Phase 4: Async read repairs
        for (const auto& [chunk_id, consensus] : consensus_map) {
            auto it = fetched_chunks.find(chunk_id);
            if (it == fetched_chunks.end()) continue;
            
            const auto& chunk_data = it->second.data;
            const auto& majority_hash = it->second.majority_hash;
            
            for (const auto& [hash, servers] : consensus.hash_to_servers) {
                if (hash != majority_hash) {
                    for (const auto& [loc, score] : servers) {
                        std::cerr << "! Data Corruption Detected! Repairing Chunk " << chunk_id << " on " << loc.server_ip << std::endl;
                        auto repair_loc = loc;
                        auto repair_data = chunk_data;
                        auto fname = filename;
                        fetch_pool().submit([this, repair_loc, fname, repair_data]() -> bool {
                            return repair_chunk_on_server(repair_loc, fname, repair_data);
                        });
                    }
                }
            }
        }
        
        std::cout << "File reconstructed successfully: " << output_path << std::endl;
        return true;
    }
    
    bool file_exists(const std::string& filename) {
        auto chunk_locations = get_chunk_locations_from_metadata(filename);
        return !chunk_locations.empty();
    }
};

static FileReconstructor g_file_reconstructor;

int process_file_download(const char* filename, const char* output_path) {
    try {
        return g_file_reconstructor.reconstruct_file(filename, output_path) ? 0 : -1;
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
