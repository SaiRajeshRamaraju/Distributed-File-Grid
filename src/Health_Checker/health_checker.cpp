#include "heart_beat_signal.hpp"
#include "system_info.hpp"
#include "version.h"
#include "../Head_Server/redis_handler.hpp"
#include "../include/config_loader.hpp"
#include <iostream>
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
    prometheus::Counter& replications_triggered_;
    prometheus::Gauge& healthy_server_count_;
    
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
                    health.is_healthy = false;
                    servers_marked_unhealthy_.Increment();
                    std::cout << "Server " << server_id << " marked as unhealthy (missed " 
                             << health.missed_heartbeats << " heartbeats)" << std::endl;
                    
                    // Trigger re-replication for failed server
                    trigger_replication(server_id);
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
            // Redirect read_entry output to capture chunk listing
            std::streambuf* orig = std::cout.rdbuf();
            std::ostringstream captured;
            std::cout.rdbuf(captured.rdbuf());
            read_entry(filename);
            std::cout.rdbuf(orig);

            std::string output = captured.str();
            // Parse for chunks on the failed server
            std::istringstream iss(output);
            std::string line;
            while (std::getline(iss, line)) {
                if (line.find("server=" + failed_prefix) != std::string::npos) {
                    // This chunk was on the failed server — extract chunk_id
                    size_t cpos = line.find("chunk:");
                    if (cpos != std::string::npos) {
                        std::string chunk_str = line.substr(cpos + 6);
                        size_t space = chunk_str.find(' ');
                        if (space != std::string::npos) {
                            int chunk_id = std::stoi(chunk_str.substr(0, space));
                            std::cout << "  -> Chunk " << chunk_id << " of file '"
                                     << filename << "' needs re-replication" << std::endl;
                            chunks_queued++;
                            // Production workflow:
                            //   a) Fetch this chunk from another healthy replica
                            //   b) Send it to a target in healthy_targets
                            //   c) Update metadata with the new location
                        }
                    }
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
        health.missed_heartbeats = 0;
        heartbeats_received_.Increment();
        
        if (!health.is_healthy) {
            health.is_healthy = true;
            std::cout << "Server " << server_id << " is back online" << std::endl;
        }
        
        std::cout << "Heartbeat from server " << server_id << " (" << health.ip 
                 << ") - CPU: " << health.cpu_usage << "%, Storage: " 
                 << health.total_storage_used << "%" << std::endl;
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
              .Register(*registry_).Add({}))
    {
        exposer_->RegisterCollectable(registry_);
    }
    void start() {
        running = true;

        std::cout << "╔═══════════════════════════════════════════════════╗" << std::endl;
        std::cout << "║    Distributed File Grid - Health Checker         ║" << std::endl;
        std::cout << "║                  Version " << APP_VERSION << "                    ║" << std::endl;
        std::cout << "╚═══════════════════════════════════════════════════╝" << std::endl;
        std::cout << std::endl;
        std::cout << "Configuration:" << std::endl;
        std::cout << "  Heartbeat Timeout:      " << heartbeat_timeout_.count() << "s" << std::endl;
        std::cout << "  Max Missed Heartbeats:  " << max_missed_heartbeats_ << std::endl;
        std::cout << "  Prometheus Metrics:     0.0.0.0:9096/metrics" << std::endl;
        std::cout << std::endl;
        
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
