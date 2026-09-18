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
#include <dfg/server_registry.hpp>

extern std::unique_ptr<dfg::HealthMonitor> g_health_monitor;
extern std::unique_ptr<dfg::ServerRegistry> g_cluster_registry;

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
  int replica_count{1};
};

// ── Transfer result for tracking chunk distribution ──
struct TransferResult {
  int chunk_id;
  std::string server;
  size_t size;
  std::string checksum;
  std::string file_path;
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
    load_cluster_servers();

    std::vector<std::string> servers_copy;
    if (g_cluster_registry) {
      servers_copy = g_cluster_registry->get_addresses();
    }
    if (servers_copy.empty()) {
      servers_copy = cluster_servers;
    }

    std::vector<std::string> selected;
    
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
                            const std::string &filename,
                            std::string &out_file_path) {

    std::string ip;
    int port;
    dfg::net::parse_address(server, ip, port);
    
        int transfer_port = 8180;
        if (g_cluster_registry) {
            for (const auto& s : g_cluster_registry->get_all()) {
                if (s.address() == server || s.host == ip) {
                    if(s.transfer_port > 0) transfer_port = s.transfer_port;
                    break;
                }
            }
        }


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

    out_file_path = resp.file_path();
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
  struct TransferOutcome {
    bool success;
    std::string file_path;
  };

  struct PendingTransfer {
    std::future<TransferOutcome> result;
    int chunk_id;
    std::string server;
    std::string checksum;
    size_t size;
  };

  void submit_chunk_transfers(int chunk_id,
                              std::shared_ptr<std::vector<char>> chunk_data,
                              const std::string &filename,
                              const std::string &checksum,
                              std::vector<PendingTransfer> &pending) {
    load_cluster_servers();
    int replication_factor = config_replication_factor();
    auto selected_servers = select_servers_for_chunk(replication_factor);

    for (const auto &server : selected_servers) {
      int cid = chunk_id;
      auto data_ptr = chunk_data;
      std::string fname = filename;
      std::string srv = server;

      auto fut =
          transfer_pool().submit([this, srv, cid, data_ptr, fname]() -> TransferOutcome {
            std::string path;
            bool ok = send_chunk_to_server(srv, cid, *data_ptr, fname, path);
            return {ok, path};
          });

      pending.push_back(
          PendingTransfer{std::move(fut), cid, server, checksum, chunk_data->size()});
    }
  }

  std::vector<ChunkInfo> collect_transfers(const std::string &filename,
                                           std::vector<PendingTransfer> &pending,
                                           int total_chunks) {
    std::vector<ChunkInfo> chunks;
    std::vector<TransferResult> all_results;
    std::map<int, int> success_replicas_count;

    for (auto &p : pending) {
      TransferOutcome outcome = p.result.get();
      all_results.push_back(
          TransferResult{p.chunk_id, p.server, p.size, p.checksum, outcome.file_path, outcome.success});
      if (outcome.success) {
        success_replicas_count[p.chunk_id]++;
      }
    }

    for (const auto &r : all_results) {
      if (r.success) {
        ChunkInfo chunk_info;
        chunk_info.chunk_id = r.chunk_id;
        chunk_info.server_ip = r.server;
        chunk_info.file_path = !r.file_path.empty() ? r.file_path
                             : ("/tmp/chunks/" + r.server + "_" + filename + "_chunk_" + std::to_string(r.chunk_id));
        chunk_info.size = r.size;
        chunk_info.checksum = r.checksum;
        chunk_info.replica_count = success_replicas_count[r.chunk_id];
        chunks.push_back(chunk_info);
      }
    }

    print_chunk_distribution(filename, all_results, total_chunks);
    return chunks;
  }

  void store_metadata(const std::string &filename,
                      const std::vector<ChunkInfo> &chunks, const std::string& file_hash) {
    std::stringstream request;
    request << filename << "\n";
    request << "TTL=3600\n";
    request << "HASH=" << file_hash << "\n";
    request << "REPLICAS=" << config_replication_factor() << "\n";

    for (const auto &chunk : chunks) {
      request << chunk.chunk_id << " " << chunk.server_ip << " "
              << chunk.file_path << " " << chunk.checksum << " "
              << chunk.replica_count << "\n";
    }

    create_entry(request.str());
    std::cout << "Metadata stored for file: " << filename
              << " (" << chunks.size() << " chunks/replicas)" << std::endl;
  }
};

// Global file chunker instance
static FileChunker g_file_chunker;

void handle_client_upload(int fd, const std::string &initial_req) {
  // Expected initial_req: "UPLOAD <filename> <file_size> <num_chunks> <file_hash>"
  // or legacy: "UPLOAD <filename> <file_size> <file_hash>"
  std::istringstream iss(initial_req);
  std::vector<std::string> tokens;
  std::string t;
  while (iss >> t) tokens.push_back(t);

  std::string filename, file_hash;
  size_t file_size = 0;
  size_t num_chunks = 0;

  if (tokens.size() >= 5 && tokens[0] == "UPLOAD") {
    filename = tokens[1];
    file_size = std::stoull(tokens[2]);
    num_chunks = std::stoull(tokens[3]);
    file_hash = tokens[4];
  } else if (tokens.size() == 4 && tokens[0] == "UPLOAD") {
    filename = tokens[1];
    file_size = std::stoull(tokens[2]);
    file_hash = tokens[3];
    num_chunks = (file_size == 0) ? 0 : (file_size + config_chunk_size() - 1) / config_chunk_size();
  } else {
    std::string err = "ERROR: Invalid upload request format. Expected: UPLOAD <filename> <size> [chunks] <sha256>\n";
    ::send(fd, err.data(), err.size(), 0);
    return;
  }

  // Acknowledge ready to receive file data
  std::string ready_msg = "READY\n";
  if (::send(fd, ready_msg.data(), ready_msg.size(), 0) < 0) {
    std::cerr << "Failed to send READY to client" << std::endl;
    return;
  }

  if (file_size == 0) {
    std::string empty_hash = !file_hash.empty() ? file_hash : dfg::hash::sha256("");
    g_file_chunker.store_metadata(filename, {}, empty_hash);
    std::string ok_msg = "SUCCESS\n";
    ::send(fd, ok_msg.data(), ok_msg.size(), 0);
    std::cout << "Successfully processed 0-byte client upload for file: " << filename << std::endl;
    return;
  }

  auto read_line = [&](int sock_fd) -> std::string {
    std::string line;
    char c;
    while (::recv(sock_fd, &c, 1, 0) == 1) {
      if (c == '\n') break;
      line += c;
    }
    return line;
  };

  std::cout << "Handling upload for: " << filename << " (" << file_size
            << " bytes, " << num_chunks << " chunks)" << std::endl;

  std::vector<FileChunker::PendingTransfer> pending;
  dfg::hash::SHA256 full_file_hasher;
  size_t chunks_received = 0;
  size_t total_bytes_received = 0;

  while (chunks_received < num_chunks) {
    std::string line = read_line(fd);
    if (line.empty()) {
      std::cerr << "Client disconnected prematurely while waiting for chunk " << chunks_received << std::endl;
      return;
    }
    if (line.rfind("CHUNK ", 0) == 0) {
      std::istringstream ch_iss(line.substr(6));
      int chunk_id = 0;
      size_t chunk_size = 0;
      std::string client_chunk_hash;
      if (!(ch_iss >> chunk_id >> chunk_size >> client_chunk_hash)) {
        std::string err = "ERROR: Malformed CHUNK header: " + line + "\n";
        ::send(fd, err.data(), err.size(), 0);
        return;
      }

      auto chunk_data = std::make_shared<std::vector<char>>(chunk_size);
      size_t received = 0;
      while (received < chunk_size) {
        ssize_t n = ::recv(fd, chunk_data->data() + received, chunk_size - received, 0);
        if (n <= 0) {
          if (n < 0 && errno == EINTR) continue;
          std::cerr << "Connection dropped while receiving chunk " << chunk_id << std::endl;
          return;
        }
        received += n;
      }

      std::string calc_hash = dfg::hash::sha256(*chunk_data);
      if (calc_hash != client_chunk_hash) {
        std::cerr << "Checksum mismatch on chunk " << chunk_id
                  << " (got " << calc_hash << ", expected " << client_chunk_hash << ")" << std::endl;
        std::string err = "ERROR: Chunk checksum mismatch for chunk " + std::to_string(chunk_id) + "\n";
        ::send(fd, err.data(), err.size(), 0);
        return;
      }

      full_file_hasher.update(*chunk_data);
      total_bytes_received += chunk_size;

      // Submit concurrent replication to cluster servers
      g_file_chunker.submit_chunk_transfers(chunk_id, chunk_data, filename, calc_hash, pending);
      chunks_received++;
    } else if (line == "EOF") {
      break;
    } else {
      std::cerr << "Unexpected line from client: " << line << std::endl;
      std::string err = "ERROR: Expected CHUNK header, got: " + line + "\n";
      ::send(fd, err.data(), err.size(), 0);
      return;
    }
  }

  // Wait for all chunk replica transfers to finish across cluster servers
  auto chunks = g_file_chunker.collect_transfers(filename, pending, chunks_received);

  std::string computed_file_hash = full_file_hasher.digest();
  if (!file_hash.empty() && computed_file_hash != file_hash) {
    std::cerr << "Full file checksum mismatch for " << filename
              << " (got " << computed_file_hash << ", expected " << file_hash << ")" << std::endl;
    std::string err = "ERROR: SHA256 checksum mismatch\n";
    ::send(fd, err.data(), err.size(), 0);
    return;
  }

  if (chunks.empty() && file_size > 0) {
    std::string err = "ERROR: Failed to store chunk replicas across cluster servers\n";
    ::send(fd, err.data(), err.size(), 0);
    return;
  }

  g_file_chunker.store_metadata(filename, chunks, computed_file_hash);
  std::string ok_msg = "SUCCESS\n";
  ::send(fd, ok_msg.data(), ok_msg.size(), 0);
  std::cout << "Successfully processed client upload for file: " << filename
            << " (" << chunks_received << " chunks, " << total_bytes_received << " bytes)" << std::endl;
}

