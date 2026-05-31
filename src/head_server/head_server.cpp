// ─────────────────── Head Server ───────────────────
// Manages file metadata, receives cluster server heartbeats,
// tracks health, exposes control API for dynamic server management,
// and reports status to ZooKeeper.

#include "redis_handler.hpp"
#include "metrics.hpp"
#include "control_api.hpp"
#include <dfg/config_loader.hpp>
#include <dfg/health_monitor.hpp>
#include <dfg/server_registry.hpp>
#include <dfg/heart_beat_signal.hpp>
#include <dfg/version.hpp>

#include <cstring>
#include <iostream>
#include <string>
#include <sys/statvfs.h>
#include <thread>
#include <chrono>
#include <csignal>
#include <atomic>

// Forward declarations
int process_file_upload(const char* filepath, const char* filename);
int process_file_download(const char* filename, const char* output_path);
int check_file_exists(const char* filename);

// Global metrics instance for the head server
static std::unique_ptr<HeadServerMetrics> g_head_metrics;
HeadServerMetrics* get_head_metrics() { return g_head_metrics.get(); }

// Global health monitor and server registry
static std::unique_ptr<dfg::HealthMonitor> g_health_monitor;
static std::unique_ptr<dfg::ServerRegistry> g_cluster_registry;
static std::unique_ptr<dfg::ControlAPI> g_control_api;

// Graceful shutdown flag
static std::atomic<bool> g_head_running{true};

static void head_signal_handler(int sig) {
    std::cout << "\nHead Server received signal " << sig << ", shutting down..." << std::endl;
    g_head_running = false;
}

// ── Heartbeat receiver thread ──
// Receives UDP heartbeats from cluster servers on port 9000
static void heartbeat_receiver_thread(dfg::HealthMonitor& monitor, HeadServerMetrics* metrics) {
    int sfd = ::socket(AF_INET, SOCK_DGRAM | SOCK_CLOEXEC, 0);
    if (sfd < 0) {
        std::cerr << "Failed to create heartbeat UDP socket" << std::endl;
        return;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(9000);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if (::bind(sfd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
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
                               reinterpret_cast<sockaddr*>(&peer), &plen);
        if (n <= 0) continue;

        // Parse frame: [4-byte length][protobuf payload]
        if (n < 4) continue;
        uint32_t msg_len;
        memcpy(&msg_len, buf, 4);
        msg_len = ntohl(msg_len);
        if (static_cast<size_t>(n) < 4 + msg_len) continue;

        heart_beat::v1::HeartBeat hb;
        if (!hb.ParseFromArray(buf + 4, msg_len)) continue;

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

// ── Health check thread ──
// Periodically checks for stale heartbeats
static void health_check_thread(dfg::HealthMonitor& monitor) {
    while (g_head_running) {
        std::this_thread::sleep_for(std::chrono::seconds(15));
        monitor.check_health();
    }
}

int run_head_server(int argc, char** argv) {
    signal(SIGINT, head_signal_handler);
    signal(SIGTERM, head_signal_handler);

    // Load configuration
    auto& cfg = head_server_config();

    // Check for custom config path in args
    for (int i = 1; i < argc - 1; ++i) {
        std::string a = argv[i];
        if ((a == "-c" || a == "--config") && i + 1 < argc) {
            cfg.load(argv[i + 1]);
        }
    }

    // Handle --dump-config
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
    std::cout << "╔═══════════════════════════════════════════════════╗" << std::endl;
    std::cout << "║         Distributed File Grid - Head Server       ║" << std::endl;
    std::cout << "║                  Version " << APP_VERSION << "                    ║" << std::endl;
    std::cout << "╚═══════════════════════════════════════════════════╝" << std::endl;
    std::cout << std::endl;
    if (cfg.is_loaded()) {
        std::cout << "  Config Source:      " << cfg.filepath() << std::endl;
    } else {
        std::cout << "  \033[33mConfig Source:      DEFAULTS (no config file loaded)\033[0m" << std::endl;
    }
    std::cout << "Configuration:" << std::endl;
    std::cout << "  Port:               " << port << std::endl;
    std::cout << "  Replication Factor: " << replication_factor << std::endl;
    std::cout << "  Chunk Size:         " << (chunk_size / (1024 * 1024)) << " MB" << std::endl;
    std::cout << "  Control API Port:   " << control_port << std::endl;
#ifdef WITH_REDIS
    std::cout << "  Metadata Backend:   Redis" << std::endl;
#else
    std::cout << "  Metadata Backend:   On-disk (" << metadata_store::db_path() << ")" << std::endl;
#endif
    std::cout << std::endl;

    // Initialize health monitor
    int hb_timeout = cfg.get_int("health_checker.heartbeat_timeout", 60);
    int max_missed = cfg.get_int("health_checker.max_missed_heartbeats", 3);
    g_health_monitor = std::make_unique<dfg::HealthMonitor>(hb_timeout, max_missed);

    // Initialize dynamic server registry
    g_cluster_registry = std::make_unique<dfg::ServerRegistry>();
    
    // Load initial cluster servers from config
    auto config_servers = cfg.get_cluster_servers();
    for (const auto& srv : config_servers) {
        g_cluster_registry->add_server(srv.host, srv.port);
        g_health_monitor->register_server(srv.id, srv.host + ":" + std::to_string(srv.port));
    }
    
    if (!config_servers.empty()) {
        std::cout << "Cluster servers loaded from config:" << std::endl;
        g_cluster_registry->print_status();
    } else {
        std::cout << "\033[33mNo cluster servers in config. Use the control API to add them at runtime.\033[0m" << std::endl;
        std::cout << "  curl -X POST http://localhost:" << control_port 
                  << "/api/v1/servers/cluster -d '{\"host\":\"x.x.x.x\",\"port\":8080}'" << std::endl;
    }
    std::cout << std::endl;

    // Initialize Prometheus metrics
    try {
        g_head_metrics = std::make_unique<HeadServerMetrics>(metrics_bind);
        std::cout << "Prometheus metrics on " << metrics_bind << "/metrics" << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Warning: Could not start metrics exporter: " << e.what() << std::endl;
    }

    // Start control API
    g_control_api = std::make_unique<dfg::ControlAPI>(*g_cluster_registry, *g_health_monitor, control_port);
    g_control_api->start();

    // Start heartbeat receiver thread
    std::thread hb_thread(heartbeat_receiver_thread, std::ref(*g_health_monitor), g_head_metrics.get());
    hb_thread.detach();

    // Start health check thread
    std::thread hc_thread(health_check_thread, std::ref(*g_health_monitor));
    hc_thread.detach();

    // Start the head server daemon (initializes metadata backend)
    start_daemon();

    std::cout << "Head Server is ready on port " << port << std::endl;
    std::cout << "Dynamic server management: http://localhost:" << control_port << "/api/v1/" << std::endl;

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
#ifndef DFG_UNIFIED_BINARY
int main(int argc, char** argv) {
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
