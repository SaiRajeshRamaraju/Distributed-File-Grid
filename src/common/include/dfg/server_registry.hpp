#pragma once
// ─────────────────── Dynamic Server Registry ───────────────────
// Thread-safe registry for cluster servers and head servers.
// Supports runtime add/remove without restart.

#include <string>
#include <vector>
#include <map>
#include <mutex>
#include <atomic>
#include <chrono>
#include <iostream>
#include <sstream>
#include <iomanip>
#include <functional>

namespace dfg {

struct ServerEntry {
    int id;
    std::string host;
    int port;
    std::string status = "active";
    std::chrono::steady_clock::time_point registered_at;
    
    std::string address() const { return host + ":" + std::to_string(port); }
};

/// Callback when a server is added/removed.
using RegistryChangeCallback = std::function<void(const ServerEntry& entry, bool added)>;

class ServerRegistry {
public:
    ServerRegistry() : next_id_(1) {}

    /// Add a server. Returns assigned ID (or existing ID if already registered).
    int add_server(const std::string& host, int port) {
        std::lock_guard<std::mutex> lock(mutex_);
        
        // Check for duplicate
        for (const auto& [id, entry] : servers_) {
            if (entry.host == host && entry.port == port) {
                std::cout << "Server already registered: " << entry.address() 
                          << " (id=" << id << ")" << std::endl;
                return id;
            }
        }
        
        int id = next_id_++;
        ServerEntry entry;
        entry.id = id;
        entry.host = host;
        entry.port = port;
        entry.registered_at = std::chrono::steady_clock::now();
        servers_[id] = entry;
        
        std::cout << "Registered server [" << id << "] " << entry.address() << std::endl;
        
        if (change_cb_) change_cb_(entry, true);
        return id;
    }

    /// Remove a server by ID.
    bool remove_server(int id) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = servers_.find(id);
        if (it == servers_.end()) return false;
        
        auto entry = it->second;
        servers_.erase(it);
        std::cout << "Deregistered server [" << id << "] " << entry.address() << std::endl;
        
        if (change_cb_) change_cb_(entry, false);
        return true;
    }

    /// Get all servers as a list of "host:port" strings.
    std::vector<std::string> get_addresses() const {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<std::string> addrs;
        for (const auto& [_, entry] : servers_) {
            if (entry.status == "active") {
                addrs.push_back(entry.address());
            }
        }
        return addrs;
    }

    /// Get all server entries.
    std::vector<ServerEntry> get_all() const {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<ServerEntry> result;
        for (const auto& [_, entry] : servers_) {
            result.push_back(entry);
        }
        return result;
    }

    /// Load initial servers from config (called at startup).
    void load_from_config(const std::vector<std::string>& addresses) {
        for (const auto& addr : addresses) {
            size_t pos = addr.find(':');
            if (pos != std::string::npos) {
                std::string host = addr.substr(0, pos);
                int port = std::stoi(addr.substr(pos + 1));
                add_server(host, port);
            }
        }
    }

    /// Set the ID counter (useful when loading from config with known IDs).
    void set_next_id(int id) { next_id_ = id; }

    int count() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return static_cast<int>(servers_.size());
    }

    void set_change_callback(RegistryChangeCallback cb) { change_cb_ = std::move(cb); }

    /// Print a formatted table of all registered servers.
    void print_status() const {
        std::lock_guard<std::mutex> lock(mutex_);
        std::cout << std::endl;
        std::cout << "╔══════╤════════════════════════╤══════════╗" << std::endl;
        std::cout << "║  ID  │ Address                │ Status   ║" << std::endl;
        std::cout << "╠══════╪════════════════════════╪══════════╣" << std::endl;
        for (const auto& [id, entry] : servers_) {
            std::cout << "║ " << std::left << std::setw(4) << id 
                      << " │ " << std::setw(22) << entry.address()
                      << " │ " << std::setw(8) << entry.status << " ║" << std::endl;
        }
        std::cout << "╚══════╧════════════════════════╧══════════╝" << std::endl;
        std::cout << "Total: " << servers_.size() << " servers" << std::endl;
    }

private:
    mutable std::mutex mutex_;
    std::map<int, ServerEntry> servers_;
    std::atomic<int> next_id_;
    RegistryChangeCallback change_cb_;
};

} // namespace dfg
