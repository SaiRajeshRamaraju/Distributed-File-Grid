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
#include <future>
#include <functional>
#include <queue>
#include <condition_variable>
#include <atomic>
#include "file_transfer.pb.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>

namespace fs = std::filesystem;

const int MAX_CONCURRENT_FETCHES = 6; // Max parallel fetch operations

struct ChunkLocation {
    int chunk_id;
    std::string server_ip;
    std::string file_path;
};

// ─────────────────── Fetch Thread Pool ───────────────────
class FetchPool {
public:
    explicit FetchPool(size_t num_threads) : stop_(false) {
        for (size_t i = 0; i < num_threads; ++i) {
            workers_.emplace_back([this] { worker_loop(); });
        }
    }

    ~FetchPool() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stop_ = true;
        }
        cv_.notify_all();
        for (auto& t : workers_) {
            if (t.joinable()) t.join();
        }
    }

    template <typename F>
    auto submit(F&& fn) -> std::future<decltype(fn())> {
        using Ret = decltype(fn());
        auto task = std::make_shared<std::packaged_task<Ret()>>(std::forward<F>(fn));
        auto fut = task->get_future();
        {
            std::lock_guard<std::mutex> lock(mutex_);
            jobs_.push([task]() { (*task)(); });
        }
        cv_.notify_one();
        return fut;
    }

private:
    void worker_loop() {
        while (true) {
            std::function<void()> job;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                cv_.wait(lock, [this] { return stop_ || !jobs_.empty(); });
                if (stop_ && jobs_.empty()) return;
                job = std::move(jobs_.front());
                jobs_.pop();
            }
            job();
        }
    }

    std::vector<std::thread> workers_;
    std::queue<std::function<void()>> jobs_;
    std::mutex mutex_;
    std::condition_variable cv_;
    bool stop_;
};

static FetchPool& fetch_pool() {
    static FetchPool pool(MAX_CONCURRENT_FETCHES);
    return pool;
}

class FileReconstructor {
private:
    // ── Non-blocking socket helpers ──
    static bool send_all_nb(int sock, const void* buf, size_t len) {
        const uint8_t* p = static_cast<const uint8_t*>(buf);
        size_t sent = 0;
        while (sent < len) {
            ssize_t n = ::send(sock, p + sent, len - sent, MSG_NOSIGNAL);
            if (n > 0) {
                sent += static_cast<size_t>(n);
            } else if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                fd_set wfds;
                FD_ZERO(&wfds);
                FD_SET(sock, &wfds);
                struct timeval tv = {10, 0};
                int rc = ::select(sock + 1, nullptr, &wfds, nullptr, &tv);
                if (rc <= 0) return false;
            } else {
                return false;
            }
        }
        return true;
    }

    static bool recv_all_nb(int sock, void* buf, size_t len) {
        uint8_t* p = static_cast<uint8_t*>(buf);
        size_t got = 0;
        while (got < len) {
            ssize_t n = ::recv(sock, p + got, len - got, 0);
            if (n > 0) {
                got += static_cast<size_t>(n);
            } else if (n == 0) {
                return false;
            } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
                fd_set rfds;
                FD_ZERO(&rfds);
                FD_SET(sock, &rfds);
                struct timeval tv = {10, 0};
                int rc = ::select(sock + 1, &rfds, nullptr, nullptr, &tv);
                if (rc <= 0) return false;
            } else {
                return false;
            }
        }
        return true;
    }

    static int connect_nb(const std::string& ip, int transfer_port, int timeout_sec = 5) {
        int sock = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0);
        if (sock < 0) return -1;

        struct sockaddr_in serv_addr{};
        serv_addr.sin_family = AF_INET;
        serv_addr.sin_port = htons(transfer_port);
        if (::inet_pton(AF_INET, ip.c_str(), &serv_addr.sin_addr) <= 0) {
            ::close(sock);
            return -1;
        }

        int rc = ::connect(sock, reinterpret_cast<sockaddr*>(&serv_addr), sizeof(serv_addr));
        if (rc < 0 && errno == EINPROGRESS) {
            fd_set wfds;
            FD_ZERO(&wfds);
            FD_SET(sock, &wfds);
            struct timeval tv = {timeout_sec, 0};
            rc = ::select(sock + 1, nullptr, &wfds, nullptr, &tv);
            if (rc <= 0) {
                ::close(sock);
                return -1;
            }
            int err = 0;
            socklen_t len = sizeof(err);
            if (::getsockopt(sock, SOL_SOCKET, SO_ERROR, &err, &len) < 0 || err != 0) {
                ::close(sock);
                return -1;
            }
        } else if (rc < 0) {
            ::close(sock);
            return -1;
        }

        return sock;
    }

    // Helper to parse ip:port from server_ip string
    static void parse_server(const std::string& server_ip, std::string& ip, int& port) {
        ip = server_ip;
        port = 8080;
        size_t pos = server_ip.find(':');
        if (pos != std::string::npos) {
            port = std::stoi(server_ip.substr(pos + 1));
            ip = server_ip.substr(0, pos);
        }
    }

    std::vector<ChunkLocation> get_chunk_locations_from_redis(const std::string& filename) {
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
            
            // Parse the output to extract chunk locations
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
    
    // ── Async hash fetch: non-blocking sockets with timeout ──
    file_transfer::v1::FetchHashResponse get_chunk_hash(const ChunkLocation& location, const std::string& filename) {
        file_transfer::v1::FetchHashResponse resp;
        resp.set_success(false);

        std::string ip;
        int port;
        parse_server(location.server_ip, ip, port);
        int transfer_port = port + 100;
        
        int sock = connect_nb(ip, transfer_port);
        if (sock < 0) return resp;

        file_transfer::v1::FetchHashRequest req;
        std::string unique_chunk_id = filename + "_chunk_" + std::to_string(location.chunk_id);
        req.set_chunk_id(unique_chunk_id);

        std::string serialized;
        req.SerializeToString(&serialized);
        uint32_t type = htonl(3);
        uint32_t len = htonl(serialized.size());
        
        if (!send_all_nb(sock, &type, sizeof(type)) ||
            !send_all_nb(sock, &len, sizeof(len)) ||
            !send_all_nb(sock, serialized.data(), serialized.size())) {
            ::close(sock);
            return resp;
        }
        
        uint32_t resp_len_net;
        if (!recv_all_nb(sock, &resp_len_net, sizeof(resp_len_net))) {
            ::close(sock);
            return resp;
        }
        
        uint32_t resp_len = ntohl(resp_len_net);
        std::vector<char> resp_buf(resp_len);
        
        if (!recv_all_nb(sock, resp_buf.data(), resp_len)) {
            ::close(sock);
            return resp;
        }
        
        resp.ParseFromArray(resp_buf.data(), resp_len);
        ::close(sock);
        return resp;
    }

    // ── Async repair: non-blocking ──
    bool repair_chunk_on_server(const ChunkLocation& location, const std::string& filename, const std::vector<char>& chunk_data) {
        std::string ip;
        int port;
        parse_server(location.server_ip, ip, port);
        int transfer_port = port + 100;
        
        int sock = connect_nb(ip, transfer_port);
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
        
        if (!send_all_nb(sock, &type, sizeof(type)) ||
            !send_all_nb(sock, &len, sizeof(len)) ||
            !send_all_nb(sock, serialized.data(), serialized.size())) {
            ::close(sock);
            return false;
        }
        
        uint32_t resp_len_net;
        if (recv_all_nb(sock, &resp_len_net, sizeof(resp_len_net))) {
            uint32_t resp_len = ntohl(resp_len_net);
            std::vector<char> resp_buf(resp_len);
            if (recv_all_nb(sock, resp_buf.data(), resp_len)) {
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

    // ── Async chunk fetch: non-blocking ──
    std::vector<char> read_chunk_from_server(const ChunkLocation& location, const std::string& filename) {
        std::vector<char> chunk_data;
        
        std::string ip;
        int port;
        parse_server(location.server_ip, ip, port);
        int transfer_port = port + 100;
        
        int sock = connect_nb(ip, transfer_port);
        if (sock < 0) return chunk_data;

        file_transfer::v1::FetchChunkRequest req;
        std::string unique_chunk_id = filename + "_chunk_" + std::to_string(location.chunk_id);
        req.set_chunk_id(unique_chunk_id);

        std::string serialized;
        req.SerializeToString(&serialized);

        uint32_t type = htonl(2);
        uint32_t len = htonl(serialized.size());
        
        if (!send_all_nb(sock, &type, sizeof(type)) ||
            !send_all_nb(sock, &len, sizeof(len)) ||
            !send_all_nb(sock, serialized.data(), serialized.size())) {
            ::close(sock);
            return chunk_data;
        }
        
        uint32_t resp_len_net;
        if (!recv_all_nb(sock, &resp_len_net, sizeof(resp_len_net))) {
            std::cerr << "Failed to receive fetch response length" << std::endl;
            ::close(sock);
            return chunk_data;
        }
        
        uint32_t resp_len = ntohl(resp_len_net);
        std::vector<char> resp_buf(resp_len);
        
        if (!recv_all_nb(sock, resp_buf.data(), resp_len)) {
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
        
        // ── Phase 1: Fetch all hashes concurrently ──
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
        
        // Collect hash results by chunk_id
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
        
        // ── Phase 2: Select best servers and fetch chunk payloads concurrently ──
        struct ChunkFetchPlan {
            int chunk_id;
            ChunkLocation best_server;
            std::string majority_hash;
        };
        
        std::vector<ChunkFetchPlan> fetch_plans;
        
        for (const auto& [chunk_id, consensus] : consensus_map) {
            if (consensus.hash_counts.empty()) {
                std::cerr << "Could not verify any hashes for chunk " << chunk_id << " from any replica!" << std::endl;
                return false;
            }
            
            // Find majority hash
            std::string majority_hash;
            int max_count = 0;
            for (const auto& [hash, count] : consensus.hash_counts) {
                if (count > max_count) {
                    max_count = count;
                    majority_hash = hash;
                }
            }
            
            // Select healthiest server with majority hash
            auto it = consensus.hash_to_servers.find(majority_hash);
            if (it == consensus.hash_to_servers.end() || it->second.empty()) {
                std::cerr << "No servers available for majority hash of chunk " << chunk_id << std::endl;
                return false;
            }
            
            auto majority_servers = it->second;
            std::sort(majority_servers.begin(), majority_servers.end(), [](const auto& a, const auto& b) {
                return a.second < b.second;
            });
            
            ChunkLocation best_server = majority_servers.front().first;
            std::cout << "Consensus for chunk " << chunk_id << ": " << majority_hash 
                      << " (Selected best server " << best_server.server_ip << " with load score " 
                      << majority_servers.front().second << ")" << std::endl;
            
            fetch_plans.push_back(ChunkFetchPlan{chunk_id, best_server, majority_hash});
        }
        
        // Fire all chunk fetches concurrently
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
        
        std::cout << "Fetching " << fetch_futures.size() << " chunk payloads concurrently..." << std::endl;
        
        // Collect all fetched chunks
        std::map<int, ChunkFetchResult> fetched_chunks;
        for (auto& fut : fetch_futures) {
            auto result = fut.get();
            if (result.data.empty()) {
                std::cerr << "Failed to fetch chunk " << result.chunk_id << std::endl;
                return false;
            }
            fetched_chunks[result.chunk_id] = std::move(result);
        }
        
        // ── Phase 3: Write output file using POSIX I/O ──
        int out_fd = ::open(output_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
        if (out_fd < 0) {
            std::cerr << "Failed to create output file: " << output_path << std::endl;
            return false;
        }
        
        // Write chunks in order
        for (const auto& [chunk_id, result] : fetched_chunks) {
            size_t written = 0;
            while (written < result.data.size()) {
                ssize_t n = ::write(out_fd, result.data.data() + written, result.data.size() - written);
                if (n < 0) {
                    if (errno == EINTR) continue;
                    std::cerr << "Write error for chunk " << chunk_id << ": " << strerror(errno) << std::endl;
                    ::close(out_fd);
                    fs::remove(output_path);
                    return false;
                }
                written += static_cast<size_t>(n);
            }
        }
        
        ::fdatasync(out_fd);
        ::close(out_fd);
        
        // ── Phase 4: Async read repairs (fire and forget via thread pool) ──
        for (const auto& [chunk_id, consensus] : consensus_map) {
            auto it = fetched_chunks.find(chunk_id);
            if (it == fetched_chunks.end()) continue;
            
            const auto& chunk_data = it->second.data;
            const auto& majority_hash = it->second.majority_hash;
            
            for (const auto& [hash, servers] : consensus.hash_to_servers) {
                if (hash != majority_hash) {
                    for (const auto& [loc, score] : servers) {
                        std::cerr << "! Data Corruption Detected! Repairing Chunk " << chunk_id << " on " << loc.server_ip << std::endl;
                        // Fire repair asynchronously - don't block the download
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
