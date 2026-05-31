#include <dfg/version.hpp>
#include <dfg/zookeeper_client.hpp>
#include <dfg/config_loader.hpp>
#include <iostream>
#include <string>
#include <thread>
#include <chrono>
#include <mutex>
#include <atomic>
#include <vector>
#include <map>
#include <unordered_map>
#include <signal.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <cstring>
#include <sstream>
#include <netdb.h>
#include <fstream>
#include <memory>
#include <functional>
#include <prometheus/exposer.h>
#include <prometheus/registry.h>
#include <prometheus/gauge.h>
#include <prometheus/counter.h>

namespace {
const char* zerror(int code) {
    switch (code) {
        case ZOK:
            return "OK";
        case ZNODEEXISTS:
            return "Node already exists";
        case ZNONODE:
            return "Node not found";
        case ZINVALIDSTATE:
            return "Invalid client state";
        default: {
            thread_local std::string buffer;
            buffer = "Unknown error (" + std::to_string(code) + ")";
            return buffer.c_str();
        }
    }
}
} // namespace

std::string get_local_ip_address();

// Real ZooKeeper client wrapper
class ZooKeeperClientWrapper {
private:
    std::unique_ptr<ZooKeeperClient> zk_client_;
    std::string connection_string_;
    std::string session_id_;
    std::atomic<bool> connected_{false};
    std::mutex mutex_;
    
public:
    ZooKeeperClientWrapper(const std::string& connection_string) 
        : connection_string_(connection_string) {
        zk_client_ = std::make_unique<ZooKeeperClient>(connection_string, 10000);
        connect();
    }
    
    bool connect() {
        if (connected_) return true;
        
        zk_client_->set_watch_callback([this](int type, int state, const char* path) {
            handle_watch_event(type, state, path);
        });
        
        if (zk_client_->connect()) {
            connected_ = true;
            std::cout << "Connected to ZooKeeper at: " << connection_string_ << std::endl;
            return true;
        }
        
        std::cerr << "Failed to connect to ZooKeeper at: " << connection_string_ << std::endl;
        return false;
    }
    
    bool create_node(const std::string& path, const std::string& data, bool ephemeral = false) {
        if (!connected_ && !connect()) return false;
        
        // Create parent nodes if they don't exist
        size_t pos = path.find_last_of('/');
        if (pos != std::string::npos && pos != 0) {
            std::string parent = path.substr(0, pos);
            if (!node_exists(parent)) {
                create_node(parent, "", false);
            }
        }
        
        int rc = zk_client_->create_node(path, data, ephemeral, false);
        if (rc == ZOK || rc == ZNODEEXISTS) {
            return true;
        }
        
        std::cerr << "Failed to create node " << path << ": " << zerror(rc) << std::endl;
        return false;
    }
    
    bool update_node(const std::string& path, const std::string& data) {
        if (!connected_ && !connect()) return false;
        
        int rc = zk_client_->set_node_data(path, data);
        if (rc == ZOK) {
            return true;
        }
        
        std::cerr << "Failed to update node " << path << ": " << zerror(rc) << std::endl;
        return false;
    }
    
    std::string get_node_data(const std::string& path) {
        if (!connected_ && !connect()) return "";
        
        return zk_client_->get_node_data(path, true);
    }
    
    bool node_exists(const std::string& path) {
        if (!connected_ && !connect()) return false;
        
        return zk_client_->node_exists(path);
    }
    
    bool delete_node(const std::string& path) {
        if (!connected_ && !connect()) return false;
        
        int rc = zk_client_->delete_node(path);
        if (rc == ZOK) {
            return true;
        }
        
        std::cerr << "Failed to delete node " << path << ": " << zerror(rc) << std::endl;
        return false;
    }
    
    std::vector<std::string> list_children(const std::string& parent_path) {
        if (!connected_ && !connect()) return {};
        
        return zk_client_->get_children(parent_path, true);
    }
    
    void cleanup_ephemeral_nodes() {
        // WARNING: In a real implementation, ephemeral nodes are automatically removed by ZooKeeper
        // when the session ends, so we don't need to do anything here.
    }
    
private:
    void handle_watch_event(int type, int state, const char* path) {
        if (state == ZOO_CONNECTED_STATE) {
            connected_ = true;
            std::cout << "ZooKeeper connection state: CONNECTED" << std::endl;
        } else if (state == ZOO_EXPIRED_SESSION_STATE) {
            connected_ = false;
            std::cerr << "ZooKeeper session expired. Attempting to reconnect..." << std::endl;
            // Try to reconnect
            std::this_thread::sleep_for(std::chrono::seconds(1));
            connect();
        } else if (state == ZOO_AUTH_FAILED_STATE) {
            std::cerr << "ZooKeeper authentication failed" << std::endl;
            connected_ = false;
        } else if (state == ZOO_CONNECTING_STATE) {
            std::cout << "ZooKeeper connection state: CONNECTING" << std::endl;
        } else if (state == ZOO_ASSOCIATING_STATE) {
            std::cout << "ZooKeeper connection state: ASSOCIATING" << std::endl;
        }
        
        // Handle node events
        if (path) {
            std::cout << "ZooKeeper watch event: type=" << type 
                     << ", state=" << state 
                     << ", path=" << path << std::endl;
        }
    }
};

struct HeadServerInfo {
    std::string server_id;
    std::string ip_address;
    int port;
    std::chrono::steady_clock::time_point last_heartbeat;
    std::chrono::steady_clock::time_point last_health_check;
    bool is_leader = false;
    std::string status = "unknown";
    double cpu_usage = 0.0;
    double memory_usage = 0.0;
    int active_connections = 0;
    int consecutive_failures = 0;
    
    HeadServerInfo() : last_health_check(std::chrono::steady_clock::now()) {}
};

class ZooKeeperHeadServerMonitor {
private:
    std::unique_ptr<ZooKeeperClientWrapper> zk_client;
    std::map<std::string, HeadServerInfo> head_servers;
    std::mutex servers_mutex;
    std::atomic<bool> running{false};
    std::string monitor_id;
    std::string leader_server_id;
    
    const std::string ZK_ROOT_PATH = "/distributed_file_grid";
    const std::string HEAD_SERVERS_PATH = ZK_ROOT_PATH + "/head_servers";
    const std::string MONITORS_PATH = ZK_ROOT_PATH + "/monitors";
    const std::chrono::seconds HEARTBEAT_TIMEOUT{30};
    const std::chrono::seconds MONITOR_INTERVAL{10};

    // Prometheus metrics
    std::shared_ptr<prometheus::Registry> registry_;
    std::unique_ptr<prometheus::Exposer> exposer_;
    prometheus::Counter& leader_elections_total_;
    prometheus::Counter& health_checks_total_;
    prometheus::Counter& health_check_failures_total_;
    prometheus::Counter& zk_reconnections_total_;
    prometheus::Gauge& zk_connected_;
    prometheus::Gauge& total_head_servers_;
    prometheus::Gauge& healthy_head_servers_;
    prometheus::Gauge& has_leader_;
    prometheus::Family<prometheus::Gauge>& per_server_status_;
    prometheus::Family<prometheus::Gauge>& per_server_cpu_;
    prometheus::Family<prometheus::Gauge>& per_server_memory_;
    
    bool check_head_server_health(HeadServerInfo& server) {
        const int MAX_RETRIES = 3;
        const std::chrono::seconds CONNECTION_TIMEOUT{5};
        
        // If we have a recent successful check, consider it healthy
        auto now = std::chrono::steady_clock::now();
        auto time_since_last_check = now - server.last_health_check;
        
        // If we checked recently and it was healthy, return early
        if (time_since_last_check < std::chrono::seconds(5) && server.status == "healthy") {
            return true;
        }
        
        // Track consecutive failures
        static std::unordered_map<std::string, int> failure_counts;
        static std::mutex failure_mutex;
        
        std::string server_key = server.server_id + ":" + server.ip_address + ":" + std::to_string(server.port);
        
        for (int attempt = 0; attempt < MAX_RETRIES; ++attempt) {
            int sock = socket(AF_INET, SOCK_STREAM, 0);
            if (sock < 0) {
                std::cerr << "Failed to create socket for " << server_key << ": " << strerror(errno) << std::endl;
                continue;
            }
            
            // Set socket to non-blocking
            int flags = fcntl(sock, F_GETFL, 0);
            fcntl(sock, F_SETFL, flags | O_NONBLOCK);
            
            struct hostent *he = gethostbyname(server.ip_address.c_str());
            if (he == nullptr) {
                close(sock);
                std::cerr << "Failed to resolve hostname: " << server.ip_address << std::endl;
                continue;
            }
            
            struct sockaddr_in addr;
            memset(&addr, 0, sizeof(addr));
            addr.sin_family = AF_INET;
            addr.sin_port = htons(server.port);
            addr.sin_addr = **((struct in_addr **)he->h_addr_list);
            
            // Start connection
            int result = connect(sock, (struct sockaddr*)&addr, sizeof(addr));
            
            if (result == 0) {
                // Connection succeeded immediately
                close(sock);
                
                // Reset failure count on success
                {
                    std::lock_guard<std::mutex> lock(failure_mutex);
                    failure_counts[server_key] = 0;
                }
                
                // Update last health check time
                server.last_health_check = now;
                return true;
            } else if (errno == EINPROGRESS) {
                // Connection in progress, wait for it to complete
                fd_set write_fds;
                FD_ZERO(&write_fds);
                FD_SET(sock, &write_fds);
                
                struct timeval timeout;
                timeout.tv_sec = CONNECTION_TIMEOUT.count();
                timeout.tv_usec = 0;
                
                int select_result = select(sock + 1, nullptr, &write_fds, nullptr, &timeout);
                
                if (select_result > 0 && FD_ISSET(sock, &write_fds)) {
                    // Check if connection was successful
                    int error = 0;
                    socklen_t len = sizeof(error);
                    getsockopt(sock, SOL_SOCKET, SO_ERROR, &error, &len);
                    
                    close(sock);
                    
                    if (error == 0) {
                        // Connection succeeded
                        std::lock_guard<std::mutex> lock(failure_mutex);
                        failure_counts[server_key] = 0;
                        server.last_health_check = now;
                        return true;
                    }
                } else {
                    close(sock);
                }
            } else {
                close(sock);
            }
            
            // If we get here, the connection attempt failed
            if (attempt < MAX_RETRIES - 1) {
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }
        }
        
        // If we get here, all retries failed
        {
            std::lock_guard<std::mutex> lock(failure_mutex);
            int failures = ++failure_counts[server_key];
            
            // Only mark as unhealthy after multiple consecutive failures
            if (failures >= 3) {
                server.status = "unhealthy";
                std::cerr << "Server " << server_key << " marked as unhealthy after " << failures << " consecutive failures" << std::endl;
            } else {
                std::cerr << "Temporary failure for server " << server_key << " (" << failures << " failures so far)" << std::endl;
            }
        }
        
        return false;
    }
    
    void register_monitor() {
        std::string monitor_path = MONITORS_PATH + "/" + monitor_id;
        std::string monitor_data = "monitor_id=" + monitor_id + ",start_time=" + 
                                 std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) +
                                 ",host=" + get_local_ip_address();
        
        if (zk_client->create_node(monitor_path, monitor_data, true)) {
            std::cout << "Registered monitor: " << monitor_id << std::endl;
        } else {
            std::cerr << "Failed to register monitor: " << monitor_id << std::endl;
        }
    }
    
    void discover_head_servers() {
        auto server_children = zk_client->list_children(HEAD_SERVERS_PATH);
        
        std::lock_guard<std::mutex> lock(servers_mutex);
        
        // Clear existing servers and rediscover
        head_servers.clear();
        
        for (const auto& server_name : server_children) {
            std::string server_path = HEAD_SERVERS_PATH + "/" + server_name;
            std::string server_data = zk_client->get_node_data(server_path);
            
            if (!server_data.empty()) {
                HeadServerInfo info = parse_server_data(server_data);
                info.server_id = server_name;
                head_servers[server_name] = info;
                
                std::cout << "Discovered head server: " << server_name 
                         << " at " << info.ip_address << ":" << info.port << std::endl;
            }
        }
    }
    
    HeadServerInfo parse_server_data(const std::string& data) {
        HeadServerInfo info;
        std::istringstream iss(data);
        std::string token;
        
        while (std::getline(iss, token, ',')) {
            size_t eq_pos = token.find('=');
            if (eq_pos != std::string::npos) {
                std::string key = token.substr(0, eq_pos);
                std::string value = token.substr(eq_pos + 1);
                
                if (key == "ip") {
                    info.ip_address = value;
                } else if (key == "port") {
                    info.port = std::stoi(value);
                } else if (key == "status") {
                    info.status = value;
                } else if (key == "cpu_usage") {
                    info.cpu_usage = std::stod(value);
                } else if (key == "memory_usage") {
                    info.memory_usage = std::stod(value);
                } else if (key == "active_connections") {
                    info.active_connections = std::stoi(value);
                } else if (key == "last_update") {
                    // Parse timestamp
                    auto timestamp = std::chrono::steady_clock::time_point(
                        std::chrono::nanoseconds(std::stoll(value)));
                    info.last_heartbeat = timestamp;
                }
            }
        }
        
        return info;
    }
    
    void perform_leader_election() {
        std::lock_guard<std::mutex> lock(servers_mutex);
        
        std::string current_leader;
        HeadServerInfo* best_candidate = nullptr;
        auto now = std::chrono::steady_clock::now();
        
        // Find current leader
        for (auto& [server_id, info] : head_servers) {
            if (info.is_leader) {
                current_leader = server_id;
                
                // Check if leader is still healthy
                if (check_head_server_health(info)) {
                    // Leader is still healthy, no need for election
                    std::cout << "Current leader " << current_leader << " is still healthy" << std::endl;
                    return;
                } else {
                    // Leader is unhealthy, demote it
                    std::cout << "Current leader " << current_leader << " is unhealthy, starting election" << std::endl;
                    info.is_leader = false;
                    info.status = "unhealthy";
                    
                    // Update ZK with leader status change
                    std::string server_path = HEAD_SERVERS_PATH + "/" + info.server_id;
                    std::string server_data = "ip=" + info.ip_address + 
                                           ",port=" + std::to_string(info.port) +
                                           ",status=unhealthy" +
                                           ",last_update=" + std::to_string(now.time_since_epoch().count());
                    
                    if (zk_client->node_exists(server_path)) {
                        zk_client->update_node(server_path, server_data);
                    }
                    break;
                }
            }
        }
        
        // Find best candidate (lowest server_id among healthy servers)
        for (auto& [server_id, info] : head_servers) {
            // Skip servers that are not healthy
            if (info.status != "healthy" && !check_head_server_health(info)) {
                continue;
            }
            
            // Prefer servers with lower CPU and memory usage
            if (!best_candidate || 
                (info.cpu_usage < best_candidate->cpu_usage && 
                 info.memory_usage < best_candidate->memory_usage)) {
                best_candidate = &info;
            } else if (info.cpu_usage == best_candidate->cpu_usage && 
                      info.memory_usage == best_candidate->memory_usage &&
                      server_id < best_candidate->server_id) {
                // If resource usage is equal, fall back to server_id comparison
                best_candidate = &info;
            }
        }
        
        if (best_candidate) {
            // Mark the old leader as not leader
            if (!current_leader.empty() && head_servers.find(current_leader) != head_servers.end()) {
                head_servers[current_leader].is_leader = false;
            }
            
            // Promote new leader
            best_candidate->is_leader = true;
            best_candidate->status = "healthy";
            leader_server_id = best_candidate->server_id;
            
            // Update ZooKeeper with new leader info
            std::string leader_path = ZK_ROOT_PATH + "/leader";
            std::string leader_data = "server_id=" + leader_server_id + 
                                    ",elected_at=" + std::to_string(now.time_since_epoch().count()) +
                                    ",ip=" + best_candidate->ip_address +
                                    ",port=" + std::to_string(best_candidate->port);
            
            if (zk_client->node_exists(leader_path)) {
                zk_client->update_node(leader_path, leader_data);
            } else {
                zk_client->create_node(leader_path, leader_data);
            }
            
            // Update the server's status in ZK
            std::string server_path = HEAD_SERVERS_PATH + "/" + leader_server_id;
            std::string server_data = "ip=" + best_candidate->ip_address + 
                                   ",port=" + std::to_string(best_candidate->port) +
                                   ",status=leader" +
                                   ",cpu_usage=" + std::to_string(best_candidate->cpu_usage) +
                                   ",memory_usage=" + std::to_string(best_candidate->memory_usage) +
                                   ",active_connections=" + std::to_string(best_candidate->active_connections) +
                                   ",last_update=" + std::to_string(now.time_since_epoch().count());
            
            if (zk_client->node_exists(server_path)) {
                zk_client->update_node(server_path, server_data);
            } else {
                zk_client->create_node(server_path, server_data, true); // ephemeral
            }
            
            leader_elections_total_.Increment();
            has_leader_.Set(1);
            std::cout << "New leader elected: " << leader_server_id 
                     << " (CPU: " << best_candidate->cpu_usage 
                     << "%, Memory: " << best_candidate->memory_usage << "%)" << std::endl;
        } else {
            std::cerr << "WARNING: No healthy head servers available for leader election!" << std::endl;
            
            // If we have no leader and no healthy servers, try to elect the least unhealthy one
            HeadServerInfo* least_unhealthy = nullptr;
            for (auto& [server_id, info] : head_servers) {
                if (!least_unhealthy || 
                    (info.consecutive_failures < least_unhealthy->consecutive_failures)) {
                    least_unhealthy = &info;
                }
            }
            
            if (least_unhealthy) {
                std::cerr << "Electing least unhealthy server as leader: " 
                         << least_unhealthy->server_id 
                         << " (" << least_unhealthy->consecutive_failures 
                         << " consecutive failures)" << std::endl;
                
                least_unhealthy->is_leader = true;
                least_unhealthy->status = "degraded";
                leader_server_id = least_unhealthy->server_id;
                
                // Update ZK with degraded leader status
                std::string leader_path = ZK_ROOT_PATH + "/leader";
                std::string leader_data = "server_id=" + leader_server_id + 
                                        ",status=degraded" +
                                        ",elected_at=" + std::to_string(now.time_since_epoch().count()) +
                                        ",ip=" + least_unhealthy->ip_address +
                                        ",port=" + std::to_string(least_unhealthy->port);
                
                if (zk_client->node_exists(leader_path)) {
                    zk_client->update_node(leader_path, leader_data);
                } else {
                    zk_client->create_node(leader_path, leader_data);
                }
            }
        }
    }
    
    void monitor_loop() {
        while (running) {
            try {
                // Discover head servers
                discover_head_servers();
                
                // Perform health checks
                {
                    std::lock_guard<std::mutex> lock(servers_mutex);
                    int healthy_count = 0;
                    for (auto& [server_id, info] : head_servers) {
                        health_checks_total_.Increment();
                        bool healthy = check_head_server_health(info);
                        std::string old_status = info.status;
                        info.status = healthy ? "healthy" : "unhealthy";
                        
                        if (healthy) healthy_count++;
                        else health_check_failures_total_.Increment();
                        
                        // Update per-server Prometheus gauges
                        per_server_status_.Add({{"server", server_id}}).Set(healthy ? 1.0 : 0.0);
                        per_server_cpu_.Add({{"server", server_id}}).Set(info.cpu_usage);
                        per_server_memory_.Add({{"server", server_id}}).Set(info.memory_usage);
                        
                        if (old_status != info.status) {
                            std::cout << "Head server " << server_id << " status changed: " 
                                     << old_status << " -> " << info.status << std::endl;
                        }
                    }
                    total_head_servers_.Set(static_cast<double>(head_servers.size()));
                    healthy_head_servers_.Set(static_cast<double>(healthy_count));
                }
                
                // Perform leader election if needed
                perform_leader_election();
                
                // Clean up stale ephemeral nodes
                zk_client->cleanup_ephemeral_nodes();
                
                // Generate health report
                generate_health_report();
                
            } catch (const std::exception& e) {
                std::cerr << "Error in monitor loop: " << e.what() << std::endl;
            }
            
            std::this_thread::sleep_for(MONITOR_INTERVAL);
        }
    }
    
    void generate_health_report() {
        std::lock_guard<std::mutex> lock(servers_mutex);
        
        std::ostringstream report;
        report << "=== Head Server Health Report ===" << std::endl;
        report << "Monitor ID: " << monitor_id << std::endl;
        report << "Current Leader: " << (leader_server_id.empty() ? "None" : leader_server_id) << std::endl;
        report << "Total Head Servers: " << head_servers.size() << std::endl;
        
        int healthy_count = 0;
        for (const auto& [server_id, info] : head_servers) {
            if (info.status == "healthy") healthy_count++;
            
            report << "  Server: " << server_id 
                   << " | Status: " << info.status
                   << " | Address: " << info.ip_address << ":" << info.port
                   << " | Leader: " << (info.is_leader ? "Yes" : "No")
                   << " | CPU: " << info.cpu_usage << "%"
                   << " | Memory: " << info.memory_usage << "%"
                   << " | Connections: " << info.active_connections << std::endl;
        }
        
        report << "Healthy Servers: " << healthy_count << "/" << head_servers.size() << std::endl;
        
        // Store report in ZooKeeper
        std::string report_path = ZK_ROOT_PATH + "/health_reports/" + monitor_id;
        zk_client->create_node(ZK_ROOT_PATH + "/health_reports", "", false);
        
        if (zk_client->node_exists(report_path)) {
            zk_client->update_node(report_path, report.str());
        } else {
            zk_client->create_node(report_path, report.str());
        }
        
        std::cout << report.str() << std::endl;
    }

public:
    ZooKeeperHeadServerMonitor(const std::string& zk_connection_string)
        : registry_(std::make_shared<prometheus::Registry>()),
          exposer_(std::make_unique<prometheus::Exposer>("0.0.0.0:9097")),
          leader_elections_total_(prometheus::BuildCounter()
              .Name("zk_leader_elections_total")
              .Help("Total number of leader elections performed")
              .Register(*registry_).Add({})),
          health_checks_total_(prometheus::BuildCounter()
              .Name("zk_health_checks_total")
              .Help("Total number of health checks performed")
              .Register(*registry_).Add({})),
          health_check_failures_total_(prometheus::BuildCounter()
              .Name("zk_health_check_failures_total")
              .Help("Total number of failed health checks")
              .Register(*registry_).Add({})),
          zk_reconnections_total_(prometheus::BuildCounter()
              .Name("zk_reconnections_total")
              .Help("Total ZooKeeper reconnection attempts")
              .Register(*registry_).Add({})),
          zk_connected_(prometheus::BuildGauge()
              .Name("zk_connected")
              .Help("Whether ZooKeeper is currently connected (1=yes, 0=no)")
              .Register(*registry_).Add({})),
          total_head_servers_(prometheus::BuildGauge()
              .Name("zk_total_head_servers")
              .Help("Total number of discovered head servers")
              .Register(*registry_).Add({})),
          healthy_head_servers_(prometheus::BuildGauge()
              .Name("zk_healthy_head_servers")
              .Help("Number of healthy head servers")
              .Register(*registry_).Add({})),
          has_leader_(prometheus::BuildGauge()
              .Name("zk_has_leader")
              .Help("Whether a leader is currently elected (1=yes, 0=no)")
              .Register(*registry_).Add({})),
          per_server_status_(prometheus::BuildGauge()
              .Name("zk_head_server_status")
              .Help("Health status per head server (1=healthy, 0=unhealthy)")
              .Register(*registry_)),
          per_server_cpu_(prometheus::BuildGauge()
              .Name("zk_head_server_cpu_percent")
              .Help("CPU usage percentage per head server")
              .Register(*registry_)),
          per_server_memory_(prometheus::BuildGauge()
              .Name("zk_head_server_memory_percent")
              .Help("Memory usage percentage per head server")
              .Register(*registry_))
    {
            exposer_->RegisterCollectable(registry_);
            zk_connected_.Set(1);

            monitor_id = "monitor_" + std::to_string(getpid()) + "_" + 
                        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
            
            zk_client = std::make_unique<ZooKeeperClientWrapper>(zk_connection_string);
            
            // Initialize ZooKeeper structure
            zk_client->create_node(ZK_ROOT_PATH, "Distributed File Grid Root");
            zk_client->create_node(HEAD_SERVERS_PATH, "Head Servers Registry");
            zk_client->create_node(MONITORS_PATH, "Monitors Registry");
            zk_client->create_node(ZK_ROOT_PATH + "/health_reports", "Health Reports");
            
            std::cout << "Initialized ZooKeeper structure at: " << zk_connection_string << std::endl;
            std::cout << "Prometheus metrics available on 0.0.0.0:9097/metrics" << std::endl;
        }  
    void start() {
        running = true;
        std::cout << "Starting ZooKeeper Head Server Monitor..." << std::endl;
        
        register_monitor();
        
        // Start monitoring thread
        std::thread monitor_thread(&ZooKeeperHeadServerMonitor::monitor_loop, this);
        monitor_thread.detach();
        
        std::cout << "ZooKeeper Head Server Monitor started with ID: " << monitor_id << std::endl;
    }
    
    void stop() {
        running = false;
        std::cout << "Stopping ZooKeeper Head Server Monitor..." << std::endl;
    }
    
    void simulate_head_server_registration(const std::string& server_id, 
                                         const std::string& ip, int port) {
        std::string server_path = HEAD_SERVERS_PATH + "/" + server_id;
        std::string server_data = "ip=" + ip + ",port=" + std::to_string(port) + 
                                ",status=healthy,cpu_usage=" + 
                                std::to_string(10.0 + (std::rand() % 30)) + // Random CPU usage between 10-40%
                                ",memory_usage=" + 
                                std::to_string(40.0 + (std::rand() % 50)) + // Random memory usage between 40-90%
                                ",active_connections=" + 
                                std::to_string(std::rand() % 100) + // Random connections between 0-100
                                ",last_update=" + 
                                std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
        
        if (zk_client->create_node(server_path, server_data, true)) { // ephemeral
            std::cout << "Simulated head server registration: " << server_id 
                     << " at " << ip << ":" << port << std::endl;
        } else {
            std::cerr << "Failed to register head server: " << server_id << std::endl;
        }
    }  
    void run_interactive_mode() {
        std::cout << "\n=== ZooKeeper Head Server Monitor Interactive Mode ===" << std::endl;
        std::cout << "Commands:" << std::endl;
        std::cout << "  register <server_id> <ip> <port> - Register a head server" << std::endl;
        std::cout << "  status - Show current status" << std::endl;
        std::cout << "  leader - Show current leader" << std::endl;
        std::cout << "  quit - Exit" << std::endl;
        
        std::string line;
        while (running && std::getline(std::cin, line)) {
            std::istringstream iss(line);
            std::string command;
            iss >> command;
            
            if (command == "register") {
                std::string server_id, ip;
                int port;
                if (iss >> server_id >> ip >> port) {
                    simulate_head_server_registration(server_id, ip, port);
                } else {
                    std::cout << "Usage: register <server_id> <ip> <port>" << std::endl;
                }
            } else if (command == "status") {
                generate_health_report();
            } else if (command == "leader") {
                std::cout << "Current leader: " << (leader_server_id.empty() ? "None" : leader_server_id) << std::endl;
            } else if (command == "quit") {
                break;
            } else {
                std::cout << "Unknown command: " << command << std::endl;
            }
        }
    }
};

// Global monitor instance
static std::unique_ptr<ZooKeeperHeadServerMonitor> g_monitor;

void signal_handler(int signal) {
    std::cout << "\nReceived signal " << signal << ", shutting down..." << std::endl;
    if (g_monitor) {
        g_monitor->stop();
    }
    exit(0);
}

// Helper function to get local IP address
std::string get_local_ip_address() {
    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) {
        return "unknown";
    }
    
    // Use Google's DNS server as a dummy address
    struct sockaddr_in serv;
    memset(&serv, 0, sizeof(serv));
    serv.sin_family = AF_INET;
    serv.sin_addr.s_addr = inet_addr("8.8.8.8");
    serv.sin_port = htons(53);
    
    // Connect to the dummy address to determine the local interface
    if (connect(sock, (const struct sockaddr*)&serv, sizeof(serv)) < 0) {
        close(sock);
        return "unknown";
    }
    
    // Get the local address
    struct sockaddr_in name;
    socklen_t namelen = sizeof(name);
    if (getsockname(sock, (struct sockaddr*)&name, &namelen) < 0) {
        close(sock);
        return "unknown";
    }
    
    close(sock);
    char ip[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &name.sin_addr, ip, sizeof(ip));
    return std::string(ip);
}

int main(int argc, char* argv[]) {
    // Load configuration
    auto& cfg = zookeeper_config();

    std::string zk_hosts = cfg.get_string("zookeeper.hosts", "localhost:2181");
    bool interactive = false;
    
    // Parse command line arguments (override config)
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--zk-hosts" && i + 1 < argc) {
            zk_hosts = argv[++i];
        } else if (arg == "-i" || arg == "--interactive") {
            interactive = true;
        } else if (arg == "--help") {
            std::cout << "Usage: " << argv[0] << " [--zk-hosts HOST:PORT] [--interactive]\n"
                      << "Options:\n"
                      << "  --zk-hosts HOST:PORT  ZooKeeper connection string (default: localhost:2181)\n"
                      << "  -i, --interactive     Run in interactive mode\n"
                      << "  --help                Show this help message\n";
            return 0;
        }
    }
    
    // Print startup banner
    std::cout << "╔═══════════════════════════════════════════════════╗" << std::endl;
    std::cout << "║   Distributed File Grid - ZooKeeper Monitor      ║" << std::endl;
    std::cout << "║                  Version " << APP_VERSION << "                    ║" << std::endl;
    std::cout << "╚═══════════════════════════════════════════════════╝" << std::endl;
    std::cout << std::endl;
    if (cfg.is_loaded()) {
        std::cout << "  Config Source:          " << cfg.filepath() << std::endl;
    } else {
        std::cout << "  \033[33mConfig Source:          DEFAULTS (no config file loaded)\033[0m" << std::endl;
    }
    std::cout << "  ZooKeeper Hosts:        " << zk_hosts << std::endl;
    std::cout << "  Session Timeout:        " << cfg.get_int("zookeeper.session_timeout_ms", 10000) << "ms" << std::endl;
    std::cout << "  Monitor Interval:       " << cfg.get_int("monitor.interval_seconds", 10) << "s" << std::endl;
    std::cout << "  Heartbeat Timeout:      " << cfg.get_int("monitor.heartbeat_timeout_seconds", 30) << "s" << std::endl;
    std::cout << std::endl;

    try {
        g_monitor = std::make_unique<ZooKeeperHeadServerMonitor>(zk_hosts);
        g_monitor->start();
        
        // Register actual head servers from docker-compose.
        // We use port 9670 because the head server's TCP listener (Control API) runs on 9670.
        g_monitor->simulate_head_server_registration("head_server_1", "head-server-1", 9670);
        g_monitor->simulate_head_server_registration("head_server_2", "head-server-2", 9670);
        
        if (interactive) {
            g_monitor->run_interactive_mode();
        } else {
            // Keep running until signal
            while (true) {
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }
        }
        
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}
