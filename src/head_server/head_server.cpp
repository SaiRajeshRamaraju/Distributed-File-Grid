// ─────────────────── Head Server ───────────────────
// Manages file metadata, receives cluster server heartbeats,
// tracks health, exposes control API for dynamic server management,
// and reports status to ZooKeeper.

#include "control_api.hpp"
#include "metrics.hpp"
#include "redis_handler.hpp"
#include <dfg/config_loader.hpp>
#include <dfg/health_monitor.hpp>
#include <dfg/async_net.hpp>
#include <dfg/server_registry.hpp>
#include <dfg/version.hpp>
#include <dfg/zookeeper_client.hpp>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstring>
#include <iostream>
#include <string>
#include <sys/statvfs.h>
#include <thread>
#include <algorithm>

// Forward declarations
int process_file_upload(const char *filepath, const char *filename);
int process_file_download(const char *filename, const char *output_path);
int check_file_exists(const char *filename);
bool replicate_chunk_to_server(const std::string& source_server, int chunk_id, const std::string& target_server, const std::string& filename);


// Global metrics instance for the head server
static std::unique_ptr<HeadServerMetrics> g_head_metrics;
HeadServerMetrics *get_head_metrics() { return g_head_metrics.get(); }

// Global health monitor and server registry
std::unique_ptr<dfg::HealthMonitor> g_health_monitor;
static std::unique_ptr<dfg::ServerRegistry> g_cluster_registry;
static std::unique_ptr<dfg::ControlAPI> g_control_api;

// Graceful shutdown flag
static std::atomic<bool> g_head_running{true};

static void head_signal_handler(int sig) {
  std::cout << "\nHead Server received signal " << sig << ", shutting down..."
            << std::endl;
  g_head_running = false;
}

// ── Heartbeat receiver thread ──
// Receives UDP heartbeats from cluster servers on port 9000
static void heartbeat_receiver_thread(dfg::HealthMonitor &monitor,
                                      HeadServerMetrics *metrics) {
  int sfd = ::socket(AF_INET, SOCK_DGRAM | SOCK_CLOEXEC, 0);
  if (sfd < 0) {
    std::cerr << "Failed to create heartbeat UDP socket" << std::endl;
    return;
  }

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(9000);
  addr.sin_addr.s_addr = htonl(INADDR_ANY);

  if (::bind(sfd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
    std::cerr << "Failed to bind heartbeat receiver on port 9000" << std::endl;
    ::close(sfd);
    return;
  }

  std::cout << "Heartbeat receiver listening on UDP port 9000" << std::endl;

  // Set receive timeout so we can check g_head_running
  struct timeval tv;
  tv.tv_sec = 2;
  tv.tv_usec = 0;
  setsockopt(sfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

  uint8_t buf[8192];
  while (g_head_running) {
    sockaddr_in peer{};
    socklen_t plen = sizeof(peer);
    ssize_t n = ::recvfrom(sfd, buf, sizeof(buf), 0,
                           reinterpret_cast<sockaddr *>(&peer), &plen);
    if (n <= 0)
      continue;

    // Parse frame: [4-byte length][protobuf payload]
    if (n < 4)
      continue;
    uint32_t msg_len;
    memcpy(&msg_len, buf, 4);
    msg_len = ntohl(msg_len);
    if (static_cast<size_t>(n) < 4 + msg_len)
      continue;

    heart_beat::v1::HeartBeat hb;
    if (!hb.ParseFromArray(buf + 4, msg_len))
      continue;

    dfg::ServerHealth health;
    health.server_id = hb.server_id();
    health.ip = hb.ip();
    health.cpu_usage = hb.cpu_usage();
    health.ram_usage = hb.ram_usage();
    health.disk_usage = hb.disk_usage();
    health.network_in = hb.network_in();
    health.network_out = hb.network_out();
    health.network_in_bps = hb.network_in_bytes_per_sec();
    health.network_out_bps = hb.network_out_bytes_per_sec();

    monitor.record_heartbeat(hb.server_id(), health);

    // Update Prometheus metrics if available
    if (metrics) {
      metrics->set_healthy_servers(monitor.healthy_count());
    }
  }

  ::close(sfd);
}

// ── Re-replicate chunks on cluster server failure ──
static void rereplicate_dead_server_chunks(const std::string &dead_server_ip) {
  std::cout << "[Failover] Starting chunk re-replication for failed cluster server: "
            << dead_server_ip << std::endl;

  auto all_files = list_all_files();
  if (all_files.empty()) {
    std::cout << "[Failover] No files found in metadata store." << std::endl;
    return;
  }

  // Find candidate healthy cluster servers
  auto all_servers = g_cluster_registry->get_addresses();
  std::vector<std::string> healthy_servers;
  auto health_map = g_health_monitor->get_all_health();
  for (const auto &srv : all_servers) {
    if (srv == dead_server_ip) continue;
    std::string h_ip;
    int h_port;
    dfg::net::parse_address(srv, h_ip, h_port);
    bool ok = true;
    for (const auto &[_, h] : health_map) {
      if ((h.ip == srv || h.ip == h_ip) && !h.is_healthy) {
        ok = false;
        break;
      }
    }
    if (ok) {
      healthy_servers.push_back(srv);
    }
  }

  if (healthy_servers.empty()) {
    std::cerr << "[Failover] No healthy cluster servers available to host chunk replicas!" << std::endl;
    return;
  }

  int total_replicated = 0;
  for (const auto &filename : all_files) {
    auto file_rec = query_metadata(filename);
    if (file_rec.chunks.empty()) continue;

    bool file_updated = false;
    // Map chunk_id -> list of chunk records (to inspect replicas)
    std::map<long long, std::vector<metadata_store::ChunkRecord>> chunk_map;
    for (const auto &cr : file_rec.chunks) {
      chunk_map[cr.chunk_id].push_back(cr);
    }

    std::vector<metadata_store::ChunkRecord> updated_chunks;

    for (auto &[cid, records] : chunk_map) {
      bool has_dead_replica = false;
      std::string surviving_source_server;
      std::string dead_checksum;

      for (const auto &r : records) {
        if (r.server == dead_server_ip) {
          has_dead_replica = true;
          dead_checksum = r.checksum;
        } else if (surviving_source_server.empty()) {
          surviving_source_server = r.server;
        }
      }

      if (!has_dead_replica) {
        for (const auto &r : records) {
          updated_chunks.push_back(r);
        }
        continue;
      }

      if (surviving_source_server.empty()) {
        std::cerr << "[Failover] Chunk " << cid << " of " << filename
                  << " had only dead server replica! Cannot recover chunk data." << std::endl;
        for (const auto &r : records) {
          updated_chunks.push_back(r);
        }
        continue;
      }

      // Pick a target healthy server that does not already host this chunk
      std::string target_server;
      for (const auto &cand : healthy_servers) {
        bool already_hosts = false;
        for (const auto &r : records) {
          if (r.server == cand) {
            already_hosts = true;
            break;
          }
        }
        if (!already_hosts) {
          target_server = cand;
          break;
        }
      }

      if (target_server.empty()) {
        std::cerr << "[Failover] No distinct healthy server available for chunk " << cid << std::endl;
        for (const auto &r : records) updated_chunks.push_back(r);
        continue;
      }

      // Perform re-replication of chunk
      std::cout << "[Failover] Duplicating chunk " << cid << " of " << filename
                << " from " << surviving_source_server << " -> " << target_server << std::endl;

      bool ok = replicate_chunk_to_server(surviving_source_server, cid, target_server, filename);
      if (ok) {
        total_replicated++;
        file_updated = true;
        for (const auto &r : records) {
          if (r.server == dead_server_ip) {
            metadata_store::ChunkRecord rep_record;
            rep_record.chunk_id = cid;
            rep_record.server = target_server;
            rep_record.path = "/tmp/chunks/" + target_server + "_" + filename + "_chunk_" + std::to_string(cid);
            rep_record.checksum = dead_checksum;
            updated_chunks.push_back(rep_record);
          } else {
            updated_chunks.push_back(r);
          }
        }
      } else {
        for (const auto &r : records) updated_chunks.push_back(r);
      }
    }

    if (file_updated) {
#ifdef WITH_REDIS
      std::stringstream req;
      req << filename << "\n";
      req << "TTL=3600\n";
      if (!file_rec.file_hash.empty()) {
        req << "HASH=" << file_rec.file_hash << "\n";
      }
      for (const auto &c : updated_chunks) {
        req << c.chunk_id << " " << c.server << " " << c.path << " " << c.checksum << "\n";
      }
      create_entry(req.str());
#else
      metadata_store::replace_file_chunks(filename, file_rec.file_hash, std::move(updated_chunks));
#endif
      std::cout << "[Failover] Updated metadata for " << filename << " after re-replication." << std::endl;
    }
  }

  std::cout << "[Failover] Completed re-replication for " << dead_server_ip
            << ": " << total_replicated << " chunks duplicated." << std::endl;
}

// ── Health check thread with ZooKeeper Voting ──
// Periodically checks for stale heartbeats, verifies with ZooKeeper cluster,
// and triggers self-healing duplication if cluster server is dead.
static void health_check_thread(dfg::HealthMonitor &monitor, const std::string &zk_hosts) {
  ZooKeeperClient zk(zk_hosts);
  bool zk_connected = zk.connect();
  if (zk_connected) {
    std::cout << "[ZK Consensus] Head server connected to ZooKeeper at " << zk_hosts << std::endl;
  } else {
    std::cout << "[ZK Consensus] Could not connect to ZooKeeper at " << zk_hosts
              << ", will retry on demand." << std::endl;
  }

  while (g_head_running) {
    std::this_thread::sleep_for(std::chrono::seconds(10));
    monitor.check_health();
  }
}

static void on_server_unhealthy_callback(int server_id, const dfg::ServerHealth &health, const std::string &zk_hosts) {
  std::cout << "\n[Consensus Voting] Health monitor flagged server " << server_id
            << " (" << health.ip << ") as UNHEALTHY. Consulting ZooKeeper cluster..."
            << std::endl;

  ZooKeeperClient zk(zk_hosts);
  if (!zk.is_connected()) {
    zk.connect();
  }

  // Check ephemeral znode in ZooKeeper: /dfg/cluster_servers/server_<id>
  std::string znode = "/dfg/cluster_servers/server_" + std::to_string(server_id);
  bool zk_node_exists = zk.node_exists(znode);

  if (zk_node_exists) {
    std::cout << "[Consensus Voting] ⚠️ ZooKeeper reports node " << znode
              << " STILL EXISTS. Cluster server is likely alive or partitioned from Head Server."
              << " Deferring re-replication." << std::endl;
  } else {
    std::cout << "[Consensus Voting] ❌ ZooKeeper confirms node " << znode
              << " DOES NOT EXIST (Session Expired/Dead). Consensus reached: SERVER IS DEAD."
              << std::endl;

    // Deregister server from registry
    g_cluster_registry->remove_server(server_id);

    // Trigger re-replication of all chunks from this dead server to a different cluster server
    std::thread([ip = health.ip]() {
      rereplicate_dead_server_chunks(ip);
    }).detach();
  }
}


int run_head_server(int argc, char **argv) {
  signal(SIGINT, head_signal_handler);
  signal(SIGTERM, head_signal_handler);

  // Load configuration
  auto &cfg = head_server_config();

  // Check for custom config path in args
  for (int i = 1; i < argc - 1; ++i) {
    std::string a = argv[i];
    if ((a == "-c" || a == "--config") && i + 1 < argc) {
      cfg.load(argv[i + 1]);
    }
  }

  // Handle --dump-config. What is the reason for this to exist??
  for (int i = 1; i < argc; ++i) {
    if (std::string(argv[i]) == "--dump-config") {
      cfg.dump();
      return 0;
    }
  }

  int port = cfg.get_int("server.port", 9669);
  int replication_factor = cfg.get_int("storage.replication_factor", 3);
  long long chunk_size = cfg.get_long("storage.chunk_size", 64 * 1024 * 1024);
  std::string metrics_bind = "0.0.0.0:9095";
  int control_port = cfg.get_int("control_api.port", 9670);

  // Override port from CLI
  for (int i = 1; i < argc - 1; ++i) {
    std::string a = argv[i];
    if ((a == "-p" || a == "--port") && i + 1 < argc)
      port = std::stoi(argv[i + 1]);
  }

  // Print startup banner
  std::cout << "╔═══════════════════════════════════════════════════╗"
            << std::endl;
  std::cout << "║         Distributed File Grid - Head Server       ║"
            << std::endl;
  std::cout << "║                  Version " << APP_VERSION
            << "                    ║" << std::endl;
  std::cout << "╚═══════════════════════════════════════════════════╝"
            << std::endl;
  std::cout << std::endl;
  if (cfg.is_loaded()) {
    std::cout << "  Config Source:      " << cfg.filepath() << std::endl;
  } else {
    std::cout << "  \033[33mConfig Source:      DEFAULTS (no config file "
                 "loaded)\033[0m"
              << std::endl;
  }
  std::cout << "Configuration:" << std::endl;
  std::cout << "  Port:               " << port << std::endl;
  std::cout << "  Replication Factor: " << replication_factor << std::endl;
  std::cout << "  Chunk Size:         " << (chunk_size / (1024 * 1024)) << " MB"
            << std::endl;
  std::cout << "  Control API Port:   " << control_port << std::endl;
#ifdef WITH_REDIS
  std::cout << "  Metadata Backend:   Redis" << std::endl;
#else
  std::cout << "  Metadata Backend:   On-disk (" << metadata_store::db_path()
            << ")" << std::endl;
#endif
  std::cout << std::endl;

  // Initialize health monitor
  int hb_timeout = cfg.get_int("health_checker.heartbeat_timeout", 60);
  int max_missed = cfg.get_int("health_checker.max_missed_heartbeats", 3);
  g_health_monitor =
      std::make_unique<dfg::HealthMonitor>(hb_timeout, max_missed);

  // Initialize dynamic server registry
  g_cluster_registry = std::make_unique<dfg::ServerRegistry>();

  // Load initial cluster servers from config
  auto config_servers = cfg.get_cluster_servers();
  for (const auto &srv : config_servers) {
    g_cluster_registry->add_server(srv.host, srv.port);
    g_health_monitor->register_server(srv.id, srv.host + ":" +
                                                  std::to_string(srv.port));
  }

  if (!config_servers.empty()) {
    std::cout << "Cluster servers loaded from config:" << std::endl;
    g_cluster_registry->print_status();
  } else {
    std::cout << "\033[33mNo cluster servers in config. Use the control API to "
                 "add them at runtime.\033[0m"
              << std::endl;
    std::cout
        << "  curl -X POST http://localhost:" << control_port
        << "/api/v1/servers/cluster -d '{\"host\":\"x.x.x.x\",\"port\":8080}'"
        << std::endl;
  }
  std::cout << std::endl;

  // Initialize Prometheus metrics
  try {
    g_head_metrics = std::make_unique<HeadServerMetrics>(metrics_bind);
    std::cout << "Prometheus metrics on " << metrics_bind << "/metrics"
              << std::endl;
  } catch (const std::exception &e) {
    std::cerr << "Warning: Could not start metrics exporter: " << e.what()
              << std::endl;
  }

  // Start control API
  g_control_api = std::make_unique<dfg::ControlAPI>(
      *g_cluster_registry, *g_health_monitor, control_port);
  g_control_api->start();

  std::string zk_hosts = cfg.get_string("zookeeper.hosts", "127.0.0.1:2181");
  for (int i = 1; i < argc - 1; ++i) {
    std::string a = argv[i];
    if (a == "--zk-hosts" && i + 1 < argc) {
      zk_hosts = argv[i + 1];
    }
  }

  // Set callback on health monitor when a server misses maximum heartbeats
  g_health_monitor->set_unhealthy_callback([zk_hosts](int server_id, const dfg::ServerHealth &health) {
    on_server_unhealthy_callback(server_id, health, zk_hosts);
  });

  // Start heartbeat receiver thread
  std::thread hb_thread(heartbeat_receiver_thread, std::ref(*g_health_monitor),
                        g_head_metrics.get());
  hb_thread.detach();

  // Start health check thread with ZooKeeper voting support
  std::thread hc_thread(health_check_thread, std::ref(*g_health_monitor), zk_hosts);
  hc_thread.detach();


  // Start the head server daemon (initializes metadata backend)
  start_daemon();
  
  // Start client listener thread
  std::thread client_listener_thread([port]() {
      int sfd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
      if (sfd < 0) return;
      int yes = 1;
      setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
      sockaddr_in addr{};
      addr.sin_family = AF_INET;
      addr.sin_port = htons(port);
      addr.sin_addr.s_addr = htonl(INADDR_ANY);
      if (::bind(sfd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
          std::cerr << "Failed to bind client listener on port " << port << std::endl;
          ::close(sfd);
          return;
      }
      ::listen(sfd, 16);
      std::cout << "Client listener ready on TCP port " << port << std::endl;
      
      while (g_head_running) {
          sockaddr_in peer{};
          socklen_t plen = sizeof(peer);
          int cfd = ::accept(sfd, reinterpret_cast<sockaddr *>(&peer), &plen);
          if (cfd < 0) {
              if (errno == EINTR) continue;
              break;
          }
          if (!g_head_running) { ::close(cfd); break; }
          
          std::thread([cfd]() {
              char buf[1024];
              ssize_t n = ::recv(cfd, buf, sizeof(buf) - 1, 0);
              if (n > 0) {
                  buf[n] = 0;
                  std::string req(buf);
                  if (req.rfind("DOWNLOAD ", 0) == 0) {
                      std::string filename = req.substr(9);
                      // trim trailing newline
                      while (!filename.empty() && (filename.back() == '\n' || filename.back() == '\r')) {
                          filename.pop_back();
                      }
                      extern void handle_client_download(int fd, const std::string& filename);
                      handle_client_download(cfd, filename);
                  } else if (req.rfind("UPLOAD ", 0) == 0) {
                      // trim trailing newline
                      while (!req.empty() && (req.back() == '\n' || req.back() == '\r')) {
                          req.pop_back();
                      }
                      extern void handle_client_upload(int fd, const std::string& initial_req);
                      handle_client_upload(cfd, req);
                  }
              }
              ::close(cfd);
          }).detach();
      }
      ::close(sfd);
  });
  client_listener_thread.detach();

  std::cout << "Head Server is ready on port " << port << std::endl;
  std::cout << "Dynamic server management: http://localhost:" << control_port
            << "/api/v1/" << std::endl;

  // Keep running
  while (g_head_running) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  g_control_api->stop();
  std::cout << "Head Server shut down gracefully." << std::endl;
  return 0;
}

// Keep backward-compatible main() for the standalone head_server binary
// Standalone main() — only compiled for the separate head_server binary.
// When building the unified dfg binary, DFG_UNIFIED_BINARY is defined
// and main() is provided by src/main.cpp instead.
//
// Reason for this to exist: I don't really know whether to make this standalone
// binary and all combined all into one. So for now , I added both ways of
// creating the binaries.
//
// WARNING: This might be removed or kept and the unified binary might be
// removed.
#ifndef DFG_UNIFIED_BINARY
int main(int argc, char **argv) {
  if (argc > 1) {
    std::string arg = argv[1];
    if (arg == "-h" || arg == "--help") {
      std::cout << "Usage: head_server [OPTIONS]" << std::endl;
      std::cout << "Options:" << std::endl;
      std::cout << "  -h, --help       Show this help message" << std::endl;
      std::cout << "  -v, --version    Show version" << std::endl;
      std::cout << "  -p, --port PORT  Set server port" << std::endl;
      std::cout << "  -c, --config F   Config file path" << std::endl;
      std::cout << "  --dump-config    Dump parsed config" << std::endl;
      return 0;
    }
    if (arg == "-v" || arg == "--version") {
      std::cout << "Version: " << APP_VERSION << std::endl;
      return 0;
    }
  }
  return run_head_server(argc, argv);
}
#endif
