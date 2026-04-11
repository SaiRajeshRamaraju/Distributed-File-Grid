#include "./redis_handler.hpp"
#include "./head_metrics.hpp"
#include "../include/config_loader.hpp"
#include <cstring>
#include <iostream>
#include <string>
#include <sys/statvfs.h>
#include <thread>
#include <chrono>
#include <csignal>

// Include the generated version header
#include "../include/version.h"

// Global metrics instance for the head server
static std::unique_ptr<HeadServerMetrics> g_head_metrics;

HeadServerMetrics* get_head_metrics() { return g_head_metrics.get(); }

// Graceful shutdown flag
static volatile bool g_head_running = true;

static void head_signal_handler(int sig) {
    std::cout << "\nHead Server received signal " << sig << ", shutting down..." << std::endl;
    g_head_running = false;
}

int main(int argc, char **argv) {
  if (argc > 1) {
    std::string arg = argv[1];
    if (arg == "-h" || arg == "--help") {
      std::cout << "Usage: head_server [OPTIONS]" << std::endl;
      std::cout << "Options:" << std::endl;
      std::cout << "  -h, --help     Show this help message and exit" << std::endl;
      std::cout << "  -v, --version  Show program's version number and exit" << std::endl;
      std::cout << "  -p, --port     Set the port number" << std::endl;
      std::cout << "  -c, --config   Path to config file (default: config/head_server_config.json)" << std::endl;
      std::cout << "  --dump-config  Dump parsed configuration and exit" << std::endl;
      return 0;
    }
    if (arg == "-v" || arg == "-V" || arg == "--version") {
      std::cout << "Version: " << APP_VERSION_MAJOR << "." << APP_VERSION_MINOR
                << "." << APP_VERSION_PATCH << std::endl;
      return 0;
    }
  }

  // Set up signal handling
  signal(SIGINT, head_signal_handler);
  signal(SIGTERM, head_signal_handler);

  // Load configuration
  auto& cfg = head_server_config();

  // Check for custom config path in args
  for (int i = 1; i < argc - 1; ++i) {
    std::string a = argv[i];
    if ((a == "-c" || a == "--config") && i + 1 < argc) {
      ConfigLoader custom_cfg;
      if (custom_cfg.load(argv[i + 1])) {
        // Replace the global config (reparse)
        cfg.load(argv[i + 1]);
      }
    }
  }

  // Handle --dump-config
  for (int i = 1; i < argc; ++i) {
    if (std::string(argv[i]) == "--dump-config") {
      cfg.dump();
      return 0;
    }
  }

  // Read configuration values (with sensible defaults)
  int port = cfg.get_int("server.port", 9669);
  int max_connections = cfg.get_int("server.max_connections", 1000);
  int replication_factor = cfg.get_int("storage.replication_factor", 3);
  long long chunk_size = cfg.get_long("storage.chunk_size", 64 * 1024 * 1024);
  std::string metrics_bind = "0.0.0.0:9095";
  std::string log_level = cfg.get_string("logging.level", "INFO");

  // Override port from command line if provided
  for (int i = 1; i < argc - 1; ++i) {
    std::string a = argv[i];
    if ((a == "-p" || a == "--port") && i + 1 < argc) {
      port = std::stoi(argv[i + 1]);
    }
  }

  // Print startup banner
  std::cout << "╔═══════════════════════════════════════════════════╗" << std::endl;
  std::cout << "║         Distributed File Grid - Head Server       ║" << std::endl;
  std::cout << "║                  Version " << APP_VERSION << "                    ║" << std::endl;
  std::cout << "╚═══════════════════════════════════════════════════╝" << std::endl;
  std::cout << std::endl;
  std::cout << "Configuration:" << std::endl;
  std::cout << "  Port:               " << port << std::endl;
  std::cout << "  Max Connections:    " << max_connections << std::endl;
  std::cout << "  Replication Factor: " << replication_factor << std::endl;
  std::cout << "  Chunk Size:         " << (chunk_size / (1024 * 1024)) << " MB" << std::endl;
  std::cout << "  Log Level:          " << log_level << std::endl;
#ifdef WITH_REDIS
  std::cout << "  Metadata Backend:   Redis" << std::endl;
  std::cout << "  Redis Host:         " << cfg.get_string("redis.host", "127.0.0.1") << std::endl;
  std::cout << "  Redis Port:         " << cfg.get_int("redis.port", 6379) << std::endl;
#else
  std::cout << "  Metadata Backend:   On-disk (" << metadata_store::db_path() << ")" << std::endl;
#endif

  // Print discovered cluster servers from config
  auto servers = cfg.get_cluster_servers();
  if (!servers.empty()) {
    std::cout << "  Cluster Servers:" << std::endl;
    for (const auto& srv : servers) {
      std::cout << "    [" << srv.id << "] " << srv.host << ":" << srv.port << std::endl;
    }
  }
  std::cout << std::endl;

  // Initialize Prometheus metrics exporter
  try {
      g_head_metrics = std::make_unique<HeadServerMetrics>(metrics_bind);
      std::cout << "Prometheus metrics available on " << metrics_bind << "/metrics" << std::endl;
  } catch (const std::exception& e) {
      std::cerr << "Warning: Could not start metrics exporter: " << e.what() << std::endl;
  }

  // Start the head server daemon (initializes Redis/metadata backend)
  start_daemon();

  std::cout << "Head Server is ready and accepting connections on port " << port << std::endl;

  // Keep the process running until signal
  while (g_head_running) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  std::cout << "Head Server shut down gracefully." << std::endl;
  return 0;
}
