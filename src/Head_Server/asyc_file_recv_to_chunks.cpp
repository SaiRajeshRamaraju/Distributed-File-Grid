#include "../include/heart_beat_signal.hpp"
#include "./redis_handler.hpp"
#include "../include/config_loader.hpp"
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
#include <atomic>
#include <functional>
#include <queue>
#include <condition_variable>
#include "file_transfer.pb.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/uio.h>

namespace fs = std::filesystem;

// Read from config, with sane defaults matching the README spec.
static size_t config_chunk_size() {
    return static_cast<size_t>(head_server_config().get_long("storage.chunk_size", 64 * 1024 * 1024));
}
static int config_replication_factor() {
    return head_server_config().get_int("storage.replication_factor", 3);
}
const int MAX_CONCURRENT_TRANSFERS = 6; // Max parallel chunk transfers

struct ChunkInfo {
    int chunk_id;
    std::string server_ip;
    std::string file_path;
    size_t size;
    std::string checksum;
};

// ─────────────────── Transfer Thread Pool ───────────────────
// Dedicated pool for concurrent chunk uploads to cluster servers.
class TransferPool {
public:
    explicit TransferPool(size_t num_threads) : stop_(false) {
        for (size_t i = 0; i < num_threads; ++i) {
            workers_.emplace_back([this] { worker_loop(); });
        }
    }

    ~TransferPool() {
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

static TransferPool& transfer_pool() {
    static TransferPool pool(MAX_CONCURRENT_TRANSFERS);
    return pool;
}

class FileChunker {
private:
    std::vector<std::string> cluster_servers;

    void load_cluster_servers() {
        if (!cluster_servers.empty()) return;
        auto entries = head_server_config().get_cluster_servers();
        for (const auto& e : entries) {
            cluster_servers.push_back(e.host + ":" + std::to_string(e.port));
        }
        // Fallback defaults if config was not loaded
        if (cluster_servers.empty()) {
            cluster_servers = {"127.0.0.1:8080", "127.0.0.1:8081", "127.0.0.1:8082"};
        }
    }
    
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

    // ── Async non-blocking send helper: drains a buffer over a non-blocking socket ──
    static bool send_all_nb(int sock, const void* buf, size_t len) {
        const uint8_t* p = static_cast<const uint8_t*>(buf);
        size_t sent = 0;
        while (sent < len) {
            ssize_t n = ::send(sock, p + sent, len - sent, MSG_NOSIGNAL);
            if (n > 0) {
                sent += static_cast<size_t>(n);
            } else if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                // Poll for writability
                fd_set wfds;
                FD_ZERO(&wfds);
                FD_SET(sock, &wfds);
                struct timeval tv = {10, 0}; // 10s timeout
                int rc = ::select(sock + 1, nullptr, &wfds, nullptr, &tv);
                if (rc <= 0) return false;
            } else {
                return false;
            }
        }
        return true;
    }

    // ── Async non-blocking recv helper ──
    static bool recv_all_nb(int sock, void* buf, size_t len) {
        uint8_t* p = static_cast<uint8_t*>(buf);
        size_t got = 0;
        while (got < len) {
            ssize_t n = ::recv(sock, p + got, len - got, 0);
            if (n > 0) {
                got += static_cast<size_t>(n);
            } else if (n == 0) {
                return false; // peer closed
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

    // ── Connect with timeout using non-blocking socket ──
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
        
        // Non-blocking connect with timeout
        int sock = connect_nb(ip, transfer_port);
        if (sock < 0) {
            std::cerr << "Connection failed to transfer port " << ip << ":" << transfer_port << std::endl;
            return false;
        }

        file_transfer::v1::ChunkData msg;
        std::string unique_chunk_id = filename + "_chunk_" + std::to_string(chunk_id);
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
            std::cerr << "Failed to send chunk data to " << server << std::endl;
            ::close(sock);
            return false;
        }
        
        uint32_t resp_len_net;
        if (!recv_all_nb(sock, &resp_len_net, sizeof(resp_len_net))) {
            std::cerr << "Failed to receive response length from " << server << std::endl;
            ::close(sock);
            return false;
        }
        
        uint32_t resp_len = ntohl(resp_len_net);
        std::vector<char> resp_buf(resp_len);
        
        if (!recv_all_nb(sock, resp_buf.data(), resp_len)) {
            ::close(sock);
            return false;
        }
        
        file_transfer::v1::ChunkResponse resp;
        if (!resp.ParseFromArray(resp_buf.data(), resp_len) || !resp.success()) {
            std::cerr << "Server rejected chunk: " << resp.error_message() << std::endl;
            ::close(sock);
            return false;
        }

        ::close(sock);
        std::cout << "Stored chunk " << chunk_id << " on server " << server << std::endl;
        return true;
    }

public:
    std::vector<ChunkInfo> split_and_store_file(const std::string& filepath, const std::string& filename) {
        load_cluster_servers();
        std::vector<ChunkInfo> chunks;
        
        const size_t CHUNK_SIZE = config_chunk_size();
        const int replication_factor = config_replication_factor();

        // Use POSIX I/O for async-friendly file reading
        int fd = ::open(filepath.c_str(), O_RDONLY | O_CLOEXEC);
        if (fd < 0) {
            std::cerr << "Failed to open file: " << filepath << std::endl;
            return chunks;
        }
        
        // Get file size
        off_t file_size = ::lseek(fd, 0, SEEK_END);
        ::lseek(fd, 0, SEEK_SET);
        
        std::cout << "Splitting file " << filename << " (" << file_size << " bytes) into chunks..."
                  << " (chunk_size=" << (CHUNK_SIZE / (1024*1024)) << "MB, replication=" << replication_factor << ")" << std::endl;
        
        int chunk_id = 0;
        size_t bytes_read = 0;
        
        // Collect all futures for concurrent chunk transfers
        struct PendingTransfer {
            std::future<bool> result;
            int chunk_id;
            std::string server;
            std::string checksum;
            size_t size;
        };
        std::vector<PendingTransfer> pending;
        
        while (bytes_read < static_cast<size_t>(file_size)) {
            size_t current_chunk_size = std::min(CHUNK_SIZE, static_cast<size_t>(file_size) - bytes_read);
            auto chunk_data = std::make_shared<std::vector<char>>(current_chunk_size);
            
            // Read chunk using POSIX read (non-blocking friendly)
            size_t total_read = 0;
            while (total_read < current_chunk_size) {
                ssize_t n = ::read(fd, chunk_data->data() + total_read, current_chunk_size - total_read);
                if (n < 0) {
                    if (errno == EINTR) continue;
                    std::cerr << "Read error: " << strerror(errno) << std::endl;
                    ::close(fd);
                    return chunks;
                }
                if (n == 0) break;
                total_read += static_cast<size_t>(n);
            }
            bytes_read += total_read;
            
            std::string checksum = calculate_checksum(*chunk_data);
            auto selected_servers = select_servers_for_chunk(replication_factor);
            
            // Fire off all replica transfers concurrently via the thread pool
            for (const auto& server : selected_servers) {
                int cid = chunk_id;
                auto data_ptr = chunk_data; // shared_ptr keeps data alive
                std::string fname = filename;
                std::string srv = server;
                
                auto fut = transfer_pool().submit([this, srv, cid, data_ptr, fname]() -> bool {
                    return send_chunk_to_server(srv, cid, *data_ptr, fname);
                });
                
                pending.push_back(PendingTransfer{
                    std::move(fut), cid, server, checksum, total_read
                });
            }
            
            chunk_id++;
        }
        
        ::close(fd);
        
        // Wait for all concurrent transfers to complete and collect results
        std::cout << "Waiting for " << pending.size() << " concurrent chunk transfers..." << std::endl;
        
        for (auto& p : pending) {
            bool ok = p.result.get();
            if (ok) {
                ChunkInfo chunk_info;
                chunk_info.chunk_id = p.chunk_id;
                chunk_info.server_ip = p.server;
                chunk_info.file_path = "/tmp/chunks/" + p.server + "_" + filename + "_chunk_" + std::to_string(p.chunk_id);
                chunk_info.size = p.size;
                chunk_info.checksum = p.checksum;
                chunks.push_back(chunk_info);
            } else {
                std::cerr << "Transfer failed: chunk " << p.chunk_id << " to " << p.server << std::endl;
            }
        }
        
        std::cout << "File split into " << chunk_id << " chunks with " << replication_factor << "x replication" << std::endl;
        std::cout << "Successful transfers: " << chunks.size() << "/" << pending.size() << std::endl;
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