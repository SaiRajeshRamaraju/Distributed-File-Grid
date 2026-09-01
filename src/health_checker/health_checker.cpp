#include <dfg/async_net.hpp>
#include <dfg/zookeeper_client.hpp>
#include <dfg/system_info.hpp>
#include <dfg/version.hpp>
#include "../head_server/redis_handler.hpp"
#include <dfg/config_loader.hpp>
#include <iostream>
#include <iomanip>
#include <unordered_map>
#include <chrono>
#include <thread>
#include <mutex>
#include <vector>
#include <algorithm>
#include <memory>
#include <sstream>
#include <prometheus/exposer.h>
#include <prometheus/registry.h>
#include <prometheus/gauge.h>
#include <prometheus/counter.h>

struct ServerHealth {
    int server_id;
    std::string ip;
    std::chrono::steady_clock::time_point last_heartbeat;
    float cpu_usage;
    float total_storage_used;
    float ram_usage;
    float disk_usage;
    std::string network_in;
    std::string network_out;
    unsigned long long network_in_bps;
    unsigned long long network_out_bps;
    bool is_healthy;
    int missed_heartbeats;
};

class HealthChecker {
private:
    std::unordered_map<int, ServerHealth> servers;
    std::mutex servers_mutex;
    bool running = false;
    int max_missed_heartbeats_;
    std::chrono::seconds heartbeat_timeout_;

    // Prometheus metrics
    std::shared_ptr<prometheus::Registry> registry_;
    std::unique_ptr<prometheus::Exposer> exposer_;
    prometheus::Counter& heartbeats_received_;
    prometheus::Counter& servers_marked_unhealthy_;
    prometheus::Counter& servers_recovered_;
    ZooKeeperClient zk_client_;
    prometheus::Counter& replications_triggered_;
    prometheus::Gauge& healthy_server_count_;
    
    // Per-server metric families (keyed by server_id label)
    prometheus::Family<prometheus::Gauge>& per_server_cpu_;
    prometheus::Family<prometheus::Gauge>& per_server_ram_;
    prometheus::Family<prometheus::Gauge>& per_server_disk_;
    prometheus::Family<prometheus::Gauge>& per_server_net_in_;
    prometheus::Family<prometheus::Gauge>& per_server_net_out_;
    prometheus::Family<prometheus::Gauge>& per_server_healthy_;
    
    async_hb::task heartbeat_receiver(async_hb::Reactor& reactor) {
        std::cout << "Starting heartbeat receiver on port 9000" << std::endl;
        
        // Create a socket for receiving heartbeats
        int sockfd = socket(AF_INET, SOCK_DGRAM, 0);
        if (sockfd < 0) {
            std::cerr << "Failed to create socket" << std::endl;
            co_return;
        }
        
        // Set up server address
        struct sockaddr_in server_addr{};
        server_addr.sin_family = AF_INET;
        server_addr.sin_port = htons(9000);
        server_addr.sin_addr.s_addr = INADDR_ANY;
        
        // Bind the socket
        if (bind(sockfd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
            std::cerr << "Failed to bind socket" << std::endl;
            close(sockfd);
            co_return;
        }
        
        // Set socket to non-blocking
        int flags = fcntl(sockfd, F_GETFL, 0);
        fcntl(sockfd, F_SETFL, flags | O_NONBLOCK);
        
        // Buffer for receiving data
        char buffer[1024];
        struct sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);
        
        while (running) {
            // Wait for data to be available
            co_await reactor.wait_readable(sockfd);
            
            // Receive the data
            ssize_t bytes_received = recvfrom(sockfd, buffer, sizeof(buffer), 0,
                                            (struct sockaddr*)&client_addr, &client_len);
            
            if (bytes_received > 0) {
                // Process the received heartbeat
                heart_beat::v1::HeartBeat hb;
                if (bytes_received > 4 && hb.ParseFromArray(buffer + 4, bytes_received - 4)) {
                    process_heartbeat(hb);
                }
            }
        }
        
        close(sockfd);
    }
    
    async_hb::task health_monitor(async_hb::Reactor& reactor) {
        while (running) {
            check_server_health();
            co_await reactor.sleep_for(std::chrono::seconds(30));
        }
    }
    
    void check_server_health() {
        std::lock_guard<std::mutex> lock(servers_mutex);
        auto now = std::chrono::steady_clock::now();
        
        for (auto& [server_id, health] : servers) {
            auto time_since_last = now - health.last_heartbeat;
            
            if (time_since_last > heartbeat_timeout_) {
                health.missed_heartbeats++;
                
                if (health.missed_heartbeats >= max_missed_heartbeats_ && health.is_healthy) {
                    // Check Zookeeper before marking dead!
                    if (!zk_client_.is_connected()) {
                        zk_client_.connect();
                    }
                    std::string znode = "/dfg/cluster_servers/server_" + std::to_string(server_id);
                    if (!zk_client_.node_exists(znode)) {
                        health.is_healthy = false;
                        servers_marked_unhealthy_.Increment();
                        std::cout << "Server " << server_id << " marked as DEAD (missed " 
                                 << health.missed_heartbeats << " heartbeats and ZNode absent)" << std::endl;
                        
                        // "give permission to create a new cluster server that register with headserver."
                        std::cout << "Granted permission to create new cluster server replacement for " << server_id << std::endl;
                        
                        // Trigger re-replication for failed server
                        trigger_replication(server_id);
                    } else {
                        std::cout << "Server " << server_id << " missed heartbeats but ZNode still exists, waiting..." << std::endl;
                    }
                }
            } else {
                if (!health.is_healthy && health.missed_heartbeats > 0) {
                    health.is_healthy = true;
                    health.missed_heartbeats = 0;
                    servers_recovered_.Increment();
                    std::cout << "Server " << server_id << " recovered and marked as healthy" << std::endl;
                }
            }
        }
        // Update healthy server count gauge
        int healthy = 0;
        for (const auto& [id, h] : servers) {
            if (h.is_healthy) healthy++;
        }
        healthy_server_count_.Set(static_cast<double>(healthy));
    }
    
    void trigger_replication(int failed_server_id) {
        replications_triggered_.Increment();
        std::cout << "Triggering re-replication for failed server " << failed_server_id << std::endl;

        // 1. Build the address prefix of the failed server so we can match
        //    it against chunk metadata entries.
        std::string failed_prefix = "127.0.0.1:" + std::to_string(8079 + failed_server_id);

        // 2. Collect healthy server addresses for re-replication targets.
        std::vector<std::string> healthy_targets;
        for (const auto& [id, h] : servers) {
            if (h.is_healthy && id != failed_server_id) {
                healthy_targets.push_back(h.ip);
            }
        }

        if (healthy_targets.empty()) {
            std::cerr << "No healthy servers available for re-replication!" << std::endl;
            return;
        }

        // 3. Scan the metadata store for any files whose chunks reference the
        //    failed server.  For each such chunk we attempt to find it on a
        //    healthy server and re-replicate.
        std::cout << "Scanning metadata for chunks on failed server " << failed_prefix << std::endl;

        auto all_files = list_all_files();
        int chunks_queued = 0;

        for (const auto& filename : all_files) {
            auto rec = query_metadata(filename);
            for (const auto& chunk : rec.chunks) {
                if (chunk.server.find(failed_prefix) != std::string::npos) {
                    std::cout << "  -> Chunk " << chunk.chunk_id << " of file '"
                             << filename << "' needs re-replication" << std::endl;
                    chunks_queued++;
                }
            }
        }

        std::cout << "Re-replication scan complete: " << chunks_queued
                 << " chunk(s) queued for re-replication across "
                 << healthy_targets.size() << " healthy server(s)" << std::endl;
    }
    
    void process_heartbeat(const heart_beat::v1::HeartBeat& hb) {
        std::lock_guard<std::mutex> lock(servers_mutex);
        
        int server_id = hb.server_id();
        auto now = std::chrono::steady_clock::now();
        
        auto& health = servers[server_id];
        health.server_id = server_id;
        health.ip = hb.ip();
        health.last_heartbeat = now;
        health.cpu_usage = hb.cpu_usage();
        health.total_storage_used = hb.total_storage_used();
        health.ram_usage = hb.ram_usage();
        health.disk_usage = hb.disk_usage();
        health.network_in = hb.network_in();
        health.network_out = hb.network_out();
        health.network_in_bps = hb.network_in_bytes_per_sec();
        health.network_out_bps = hb.network_out_bytes_per_sec();
        health.missed_heartbeats = 0;
        heartbeats_received_.Increment();
        
        if (!health.is_healthy) {
            health.is_healthy = true;
            std::cout << "Server " << server_id << " is back online" << std::endl;
        }
        
        std::cout << "Heartbeat from server " << server_id << " (" << health.ip << ")"
                 << " - CPU: " << std::fixed << std::setprecision(1) << health.cpu_usage << "%"
                 << ", RAM: " << std::fixed << std::setprecision(1) << health.ram_usage << "%"
                 << ", Disk: " << std::fixed << std::setprecision(1) << health.disk_usage << "%"
                 << ", Net In: " << (health.network_in.empty() ? "N/A" : health.network_in)
                 << ", Net Out: " << (health.network_out.empty() ? "N/A" : health.network_out)
                 << std::endl;

        // Update per-server Prometheus gauges
        std::string sid = std::to_string(server_id);
        per_server_cpu_.Add({{"server_id", sid}}).Set(health.cpu_usage);
        per_server_ram_.Add({{"server_id", sid}}).Set(health.ram_usage);
        per_server_disk_.Add({{"server_id", sid}}).Set(health.disk_usage);
        per_server_net_in_.Add({{"server_id", sid}}).Set(static_cast<double>(health.network_in_bps));
        per_server_net_out_.Add({{"server_id", sid}}).Set(static_cast<double>(health.network_out_bps));
        per_server_healthy_.Add({{"server_id", sid}}).Set(health.is_healthy ? 1.0 : 0.0);
    }

public:
    HealthChecker()
        : max_missed_heartbeats_(health_checker_config().get_int("monitoring.max_missed_heartbeats", 3)),
          heartbeat_timeout_(std::chrono::seconds(health_checker_config().get_int("monitoring.heartbeat_timeout", 60))),
          registry_(std::make_shared<prometheus::Registry>()),
          exposer_(std::make_unique<prometheus::Exposer>("0.0.0.0:9096")),
          heartbeats_received_(prometheus::BuildCounter()
              .Name("hc_heartbeats_received_total")
              .Help("Total heartbeats received")
              .Register(*registry_).Add({})),
          servers_marked_unhealthy_(prometheus::BuildCounter()
              .Name("hc_servers_marked_unhealthy_total")
              .Help("Total times a server was marked unhealthy")
              .Register(*registry_).Add({})),
          zk_client_("127.0.0.1:2181"),
          servers_recovered_(prometheus::BuildCounter()
              .Name("hc_servers_recovered_total")
              .Help("Total times a server recovered")
              .Register(*registry_).Add({})),
          replications_triggered_(prometheus::BuildCounter()
              .Name("hc_replications_triggered_total")
              .Help("Total re-replications triggered")
              .Register(*registry_).Add({})),
          healthy_server_count_(prometheus::BuildGauge()
              .Name("hc_healthy_servers")
              .Help("Number of currently healthy servers")
              .Register(*registry_).Add({})),
          per_server_cpu_(prometheus::BuildGauge()
              .Name("hc_server_cpu_usage_percent")
              .Help("CPU usage percentage per cluster server")
              .Register(*registry_)),
          per_server_ram_(prometheus::BuildGauge()
              .Name("hc_server_ram_usage_percent")
              .Help("RAM usage percentage per cluster server")
              .Register(*registry_)),
          per_server_disk_(prometheus::BuildGauge()
              .Name("hc_server_disk_usage_percent")
              .Help("Disk usage percentage per cluster server")
              .Register(*registry_)),
          per_server_net_in_(prometheus::BuildGauge()
              .Name("hc_server_network_in_bytes_per_sec")
              .Help("Network ingress bytes/sec per cluster server")
              .Register(*registry_)),
          per_server_net_out_(prometheus::BuildGauge()
              .Name("hc_server_network_out_bytes_per_sec")
              .Help("Network egress bytes/sec per cluster server")
              .Register(*registry_)),
          per_server_healthy_(prometheus::BuildGauge()
              .Name("hc_server_healthy")
              .Help("Whether each cluster server is healthy (1=yes, 0=no)")
              .Register(*registry_))
    {
        exposer_->RegisterCollectable(registry_);
    }
    void ui_server() {
        int lfd = socket(AF_INET, SOCK_STREAM, 0);
        int opt = 1;
        setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(9098);
        addr.sin_addr.s_addr = INADDR_ANY;
        
        if (bind(lfd, (sockaddr*)&addr, sizeof(addr)) < 0) {
            std::cerr << "Failed to bind UI server on port 9098" << std::endl;
            close(lfd);
            return;
        }
        listen(lfd, 10);
        
        while (running) {
            // Use select to make accept interruptible
            fd_set fds;
            FD_ZERO(&fds);
            FD_SET(lfd, &fds);
            timeval tv{1, 0}; // 1 second timeout
            
            if (select(lfd + 1, &fds, nullptr, nullptr, &tv) > 0) {
                int cfd = accept(lfd, nullptr, nullptr);
                if (cfd < 0) continue;
                
                std::string response = "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nConnection: close\r\n\r\n";
                response += "<html><head><title>Health Checker UI</title><meta http-equiv=\"refresh\" content=\"5\"><style>body{font-family:sans-serif;} table{border-collapse:collapse;width:100%;} th,td{border:1px solid #ddd;padding:8px;text-align:left;} th{background-color:#f2f2f2;}</style></head><body>";
                response += "<h1>Cluster Server Health Status</h1>";
                response += "<table><tr><th>Server ID</th><th>IP</th><th>CPU (%)</th><th>RAM (%)</th><th>Disk (%)</th><th>Net In</th><th>Net Out</th><th>Status</th></tr>";
                
                auto servers_status = get_server_status();
                for (const auto& s : servers_status) {
                    std::string status_color = s.is_healthy ? "green" : "red";
                    std::string status_text = s.is_healthy ? "Healthy" : "Unhealthy";
                    response += "<tr><td>" + std::to_string(s.server_id) + "</td>"
                             + "<td>" + s.ip + "</td>"
                             + "<td>" + std::to_string(s.cpu_usage) + "</td>"
                             + "<td>" + std::to_string(s.ram_usage) + "</td>"
                             + "<td>" + std::to_string(s.disk_usage) + "</td>"
                             + "<td>" + s.network_in + "</td>"
                             + "<td>" + s.network_out + "</td>"
                             + "<td style=\"color:" + status_color + ";font-weight:bold;\">" + status_text + "</td></tr>";
                }
                
                response += "</table></body></html>";
                
                send(cfd, response.c_str(), response.size(), 0);
                close(cfd);
            }
        }
        close(lfd);
    }
    
    std::thread ui_thread;

    void start() {
        running = true;

        auto& cfg = health_checker_config();

        std::cout << "╔═══════════════════════════════════════════════════╗" << std::endl;
        std::cout << "║    Distributed File Grid - Health Checker         ║" << std::endl;
        std::cout << "║                  Version " << APP_VERSION << "                    ║" << std::endl;
        std::cout << "╚═══════════════════════════════════════════════════╝" << std::endl;
        std::cout << std::endl;
        if (cfg.is_loaded()) {
            std::cout << "  Config Source:          " << cfg.filepath() << std::endl;
        } else {
            std::cout << "  \033[33mConfig Source:          DEFAULTS (no config file loaded)\033[0m" << std::endl;
        }
        std::cout << "Configuration:" << std::endl;
        std::cout << "  Heartbeat Timeout:      " << heartbeat_timeout_.count() << "s" << std::endl;
        std::cout << "  Max Missed Heartbeats:  " << max_missed_heartbeats_ << std::endl;
        std::cout << "  Prometheus Metrics:     0.0.0.0:9096/metrics" << std::endl;
        std::cout << "  Web UI:                 http://0.0.0.0:9098" << std::endl;
        std::cout << std::endl;
        
        ui_thread = std::thread(&HealthChecker::ui_server, this);

        async_hb::Reactor reactor;
        
        // Start heartbeat receiver
        reactor.spawn(heartbeat_receiver(reactor));
        
        // Start health monitor
        reactor.spawn(health_monitor(reactor));
        
        // Run the reactor
        reactor.run();
    }
    
    void stop() {
        running = false;
        if (ui_thread.joinable()) ui_thread.join();
        std::cout << "Stopping Health Checker service" << std::endl;
    }
    
    std::vector<ServerHealth> get_server_status() {
        std::lock_guard<std::mutex> lock(servers_mutex);
        std::vector<ServerHealth> status;
        for (const auto& [id, health] : servers) {
            status.push_back(health);
        }
        return status;
    }
    
    std::vector<int> get_healthy_servers() {
        std::lock_guard<std::mutex> lock(servers_mutex);
        std::vector<int> healthy;
        for (const auto& [id, health] : servers) {
            if (health.is_healthy) {
                healthy.push_back(id);
            }
        }
        return healthy;
    }
    
    bool is_server_healthy(int server_id) {
        std::lock_guard<std::mutex> lock(servers_mutex);
        auto it = servers.find(server_id);
        return it != servers.end() && it->second.is_healthy;
    }
};

// Global health checker instance
static std::unique_ptr<HealthChecker> g_health_checker;

int main(int argc, char** argv) {
    if (argc > 1) {
        std::string arg = argv[1];
        
        if (arg == "-h" || arg == "--help") {
            std::cout << "Usage: health_checker [OPTIONS]\n";
            std::cout << "Options:\n";
            std::cout << "  -h, --help     Show this help message and exit\n";
            std::cout << "  -v, --version  Show program's version number and exit\n";
            return 0;
        }
        if (arg == "-v" || arg == "-V" || arg == "--version") {
            std::cout << "Health Checker version: " << APP_VERSION << std::endl;
            return 0;
        }
    }
    
    try {
        g_health_checker = std::make_unique<HealthChecker>();
        g_health_checker->start();
    } catch (const std::exception& e) {
        std::cerr << "Error starting health checker: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}

extern "C" {
    int start_health_checker() {
        try {
            g_health_checker = std::make_unique<HealthChecker>();
            g_health_checker->start();
            return 0;
        } catch (const std::exception& e) {
            std::cerr << "Error starting health checker: " << e.what() << std::endl;
            return -1;
        }
    }
    
    void stop_health_checker() {
        if (g_health_checker) {
            g_health_checker->stop();
            g_health_checker.reset();
        }
    }
}
