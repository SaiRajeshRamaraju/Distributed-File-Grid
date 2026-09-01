// ─────────────────── File Upload / Chunking ───────────────────
// Splits a file into chunks and transfers replicas to cluster servers.
// Uses shared net_utils and thread_pool instead of local duplicates.

#include "file_transfer.pb.h"
#include "redis_handler.hpp"
#include <dfg/config_loader.hpp>
#include <dfg/async_net.hpp>
#include <dfg/net_utils.hpp>
#include <dfg/sha256.hpp>
#include <dfg/thread_pool.hpp>
#include <dfg/health_monitor.hpp>

extern std::unique_ptr<dfg::HealthMonitor> g_health_monitor;

#include <algorithm>
#include <atomic>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <functional>
#include <future>
#include <iomanip>
#include <iostream>
#include <random>
#include <sstream>
#include <string>
#include <sys/uio.h>
#include <thread>
#include <vector>

namespace fs = std::filesystem;

// TODO: Read from config, with same defaults matching the README spec.
static size_t config_chunk_size() {
  return static_cast<size_t>(
      head_server_config().get_long("storage.chunk_size", 64 * 1024 * 1024));
}
static int config_replication_factor() {
  return head_server_config().get_int("storage.replication_factor", 3);
}
static constexpr int MAX_CONCURRENT_TRANSFERS = 6;

struct ChunkInfo {
  int chunk_id;
  std::string server_ip;
  std::string file_path;
  size_t size;
  std::string checksum;
};

// ── Transfer result for tracking chunk distribution ──
struct TransferResult {
  int chunk_id;
  std::string server;
  size_t size;
  std::string checksum;
  bool success;
};

static dfg::ThreadPool &transfer_pool() {
  static dfg::ThreadPool pool(MAX_CONCURRENT_TRANSFERS);
  return pool;
}

class FileChunker {
private:
  std::vector<std::string> cluster_servers;

  void load_cluster_servers() {
    if (!cluster_servers.empty())
      return;
    auto entries = head_server_config().get_cluster_servers();
    for (const auto &e : entries) {
      cluster_servers.push_back(e.host + ":" + std::to_string(e.port));
    }
    // Fallback defaults if config was not loaded
    if (cluster_servers.empty()) {
      cluster_servers = {"127.0.0.1:8080", "127.0.0.1:8081", "127.0.0.1:8082"};
    }
  }

  std::string calculate_checksum(const std::vector<char> &data) {
    return dfg::hash::sha256(data);
  }

  std::vector<std::string> select_servers_for_chunk(int replication_factor) {
    std::vector<std::string> selected;

    auto servers_copy = cluster_servers;
    
    // Sort by lowest disk usage and filter healthy servers
    if (g_health_monitor) {
        auto health_map = g_health_monitor->get_all_health();
        std::vector<std::string> healthy_servers;
        for (const auto &srv : servers_copy) {
            std::string ip;
            int port;
            dfg::net::parse_address(srv, ip, port);
            bool found = false;
            bool is_healthy = false;
            for (const auto& [id, health] : health_map) {
                if (health.ip == ip || health.ip == srv) {
                    found = true;
                    is_healthy = health.is_healthy;
                    break;
                }
            }
            if (!found || is_healthy) {
                healthy_servers.push_back(srv);
            }
        }
        
        std::vector<std::string> candidate_servers = !healthy_servers.empty() ? healthy_servers : servers_copy;
        
        std::sort(candidate_servers.begin(), candidate_servers.end(), [&health_map](const std::string& a, const std::string& b) {
            std::string ip_a, ip_b;
            int port_a, port_b;
            dfg::net::parse_address(a, ip_a, port_a);
            dfg::net::parse_address(b, ip_b, port_b);
            float disk_a = 100.0f;
            float disk_b = 100.0f;
            
            for (const auto& [id, health] : health_map) {
                if (health.ip == ip_a || health.ip == a) disk_a = health.disk_usage;
                if (health.ip == ip_b || health.ip == b) disk_b = health.disk_usage;
            }
            return disk_a < disk_b;
        });
        servers_copy = candidate_servers;
    }

    int count =
        std::min(replication_factor, static_cast<int>(servers_copy.size()));
    for (int i = 0; i < count; i++) {
      selected.push_back(servers_copy[i]);
    }
    return selected;
  }

  bool send_chunk_to_server(const std::string &server, int chunk_id,
                            const std::vector<char> &chunk_data,
                            const std::string &filename) {

    std::string ip;
    int port;
    dfg::net::parse_address(server, ip, port);
    int transfer_port = port + 100;

    int sock = dfg::net::connect_with_timeout(ip, transfer_port);
    if (sock < 0) {
      std::cerr << "Connection failed to transfer port " << ip << ":"
                << transfer_port << std::endl;
      return false;
    }

    file_transfer::v1::ChunkData msg;
    std::string unique_chunk_id =
        filename + "_chunk_" + std::to_string(chunk_id);
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
      std::cerr << "Failed to send chunk data to " << server << std::endl;
      ::close(sock);
      return false;
    }

    uint32_t resp_len_net;
    if (!dfg::net::recv_all(sock, &resp_len_net, sizeof(resp_len_net))) {
      std::cerr << "Failed to receive response length from " << server
                << std::endl;
      ::close(sock);
      return false;
    }

    uint32_t resp_len = ntohl(resp_len_net);
    std::vector<char> resp_buf(resp_len);

    if (!dfg::net::recv_all(sock, resp_buf.data(), resp_len)) {
      ::close(sock);
      return false;
    }

    file_transfer::v1::ChunkResponse resp;
    if (!resp.ParseFromArray(resp_buf.data(), resp_len) || !resp.success()) {
      std::cerr << "Server rejected chunk: " << resp.error_message()
                << std::endl;
      ::close(sock);
      return false;
    }

    ::close(sock);
    return true;
  }

  // ── Print chunk distribution table ──
  void print_chunk_distribution(const std::string &filename,
                                const std::vector<TransferResult> &results,
                                int total_chunks) {
    std::cout << std::endl;
    std::cout << "╔════════════════════════════════════════════════════════════"
                 "══════╗"
              << std::endl;
    std::cout << "║  Chunk Distribution Status for: " << std::left
              << std::setw(32) << filename << "║" << std::endl;
    std::cout
        << "╠════════╤══════════╤════════════════════════╤════════════════════╣"
        << std::endl;
    std::cout
        << "║ Chunk  │ Size     │ Server                 │ Status             ║"
        << std::endl;
    std::cout
        << "╠════════╪══════════╪════════════════════════╪════════════════════╣"
        << std::endl;

    int success_count = 0;
    for (const auto &r : results) {
      std::string size_str;
      if (r.size >= 1024 * 1024) {
        std::ostringstream oss;
        oss << std::fixed << std::setprecision(1)
            << (r.size / (1024.0 * 1024.0)) << " MB";
        size_str = oss.str();
      } else if (r.size >= 1024) {
        std::ostringstream oss;
        oss << std::fixed << std::setprecision(1) << (r.size / 1024.0) << " KB";
        size_str = oss.str();
      } else {
        size_str = std::to_string(r.size) + " B";
      }

      std::string status = r.success ? "✅ Stored" : "❌ Failed";
      if (r.success)
        success_count++;

      std::cout << "║ " << std::left << std::setw(6) << r.chunk_id << " │ "
                << std::setw(8) << size_str << " │ " << std::setw(22)
                << r.server << " │ " << std::setw(18) << status << " ║"
                << std::endl;
    }

    std::cout
        << "╚════════╧══════════╧════════════════════════╧════════════════════╝"
        << std::endl;

    double pct =
        results.empty() ? 0.0 : (100.0 * success_count / results.size());
    std::cout << "Summary: " << success_count << "/" << results.size()
              << " replicas stored successfully (" << std::fixed
              << std::setprecision(1) << pct << "%)" << std::endl;
    std::cout << "Total chunks: " << total_chunks
              << " | Replication factor: " << config_replication_factor()
              << std::endl;
    std::cout << std::endl;
  }

public:
  std::pair<std::vector<ChunkInfo>, std::string> split_and_store_file(const std::string &filepath,
                                              const std::string &filename) {
    load_cluster_servers();
    std::vector<ChunkInfo> chunks;
    std::string file_hash_str;

    const size_t CHUNK_SIZE = config_chunk_size();
    const int replication_factor = config_replication_factor();

    int fd = ::open(filepath.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
      std::cerr << "Failed to open file: " << filepath << std::endl;
      return {chunks, ""};
    }

    off_t file_size = ::lseek(fd, 0, SEEK_END);
    ::lseek(fd, 0, SEEK_SET);

    std::cout << "Splitting file " << filename << " (" << file_size
              << " bytes) into chunks..."
              << " (chunk_size=" << (CHUNK_SIZE / (1024 * 1024))
              << "MB, replication=" << replication_factor << ")" << std::endl;

    int chunk_id = 0;
    size_t bytes_read = 0;

    // Collect all transfer results for the status table
    struct PendingTransfer {
      std::future<bool> result;
      int chunk_id;
      std::string server;
      std::string checksum;
      size_t size;
    };
    std::vector<PendingTransfer> pending;
    
    dfg::hash::SHA256 full_file_hash;

    while (bytes_read < static_cast<size_t>(file_size)) {
      size_t current_chunk_size =
          std::min(CHUNK_SIZE, static_cast<size_t>(file_size) - bytes_read);
      auto chunk_data = std::make_shared<std::vector<char>>(current_chunk_size);

      size_t total_read = 0;
      while (total_read < current_chunk_size) {
        ssize_t n = ::read(fd, chunk_data->data() + total_read,
                           current_chunk_size - total_read);
        if (n < 0) {
          if (errno == EINTR)
            continue;
          std::cerr << "Read error: " << strerror(errno) << std::endl;
          ::close(fd);
          return {chunks, ""};
        }
        if (n == 0)
          break;
        total_read += static_cast<size_t>(n);
      }
      bytes_read += total_read;
      
      full_file_hash.update(*chunk_data);

      std::string checksum = calculate_checksum(*chunk_data);
      auto selected_servers = select_servers_for_chunk(replication_factor);

      for (const auto &server : selected_servers) {
        int cid = chunk_id;
        auto data_ptr = chunk_data;
        std::string fname = filename;
        std::string srv = server;

        auto fut =
            transfer_pool().submit([this, srv, cid, data_ptr, fname]() -> bool {
              return send_chunk_to_server(srv, cid, *data_ptr, fname);
            });

        pending.push_back(
            PendingTransfer{std::move(fut), cid, server, checksum, total_read});
      }

      chunk_id++;
    }

    ::close(fd);
    
    file_hash_str = full_file_hash.digest();

    // Wait for all concurrent transfers and collect results
    std::cout << "Waiting for " << pending.size()
              << " concurrent chunk transfers..." << std::endl;

    std::vector<TransferResult> all_results;
    for (auto &p : pending) {
      bool ok = p.result.get();
      all_results.push_back(
          TransferResult{p.chunk_id, p.server, p.size, p.checksum, ok});
      if (ok) {
        ChunkInfo chunk_info;
        chunk_info.chunk_id = p.chunk_id;
        chunk_info.server_ip = p.server;
        chunk_info.file_path = "/tmp/chunks/" + p.server + "_" + filename +
                               "_chunk_" + std::to_string(p.chunk_id);
        chunk_info.size = p.size;
        chunk_info.checksum = p.checksum;
        chunks.push_back(chunk_info);
      }
    }

    // Print chunk distribution status table
    print_chunk_distribution(filename, all_results, chunk_id);

    return {chunks, file_hash_str};
  }

  void store_metadata(const std::string &filename,
                      const std::vector<ChunkInfo> &chunks, const std::string& file_hash) {
    std::stringstream request;
    request << filename << "\n";
    request << "TTL=3600\n";
    request << "HASH=" << file_hash << "\n";

    for (const auto &chunk : chunks) {
      request << chunk.chunk_id << " " << chunk.server_ip << " "
              << chunk.file_path << " " << chunk.checksum << "\n";
    }

    create_entry(request.str());
    std::cout << "Metadata stored for file: " << filename << std::endl;
  }
};

// Global file chunker instance
static FileChunker g_file_chunker;

int process_file_upload(const char *filepath, const char *filename) {
  try {
    struct stat st;
    if (stat(filepath, &st) != 0) {
      std::cerr << "File not found: " << filepath << std::endl;
      return -1;
    }
    auto [chunks, file_hash] = g_file_chunker.split_and_store_file(filepath, filename);
    if (st.st_size > 0 && chunks.empty()) {
      return -1;
    }

    g_file_chunker.store_metadata(filename, chunks, file_hash);
    return 0;
  } catch (const std::exception &e) {
    std::cerr << "Error processing file upload: " << e.what() << std::endl;
    return -1;
  }
}
