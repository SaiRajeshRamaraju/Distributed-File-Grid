#pragma once
// ─────────────────── Embedded Health Monitor ───────────────────
// Shared health monitoring component that can be embedded in both
// Head Server and Cluster Server. Receives heartbeats, tracks server
// health, and exposes the data for upstream reporting.

#include <atomic>
#include <chrono>
#include <functional>
#include <iomanip>
#include <iostream>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

namespace dfg {

struct ServerHealth {
  int server_id = 0;
  std::string ip;
  float cpu_usage = 0.0f;
  float ram_usage = 0.0f;
  float disk_usage = 0.0f;
  std::string network_in; // TODO: Check Why do we need this?
  std::string network_out;
  uint64_t network_in_bps = 0;
  uint64_t network_out_bps = 0;
  bool is_healthy = true;
  std::chrono::steady_clock::time_point last_heartbeat;
  int missed_heartbeats = 0;
};

/// Callback invoked when a server transitions to unhealthy.
using UnhealthyCallback =
    std::function<void(int server_id, const ServerHealth &health)>;

/// Callback invoked when a server recovers.
using RecoveredCallback =
    std::function<void(int server_id, const ServerHealth &health)>;

class HealthMonitor {
public:
  explicit HealthMonitor(int heartbeat_timeout_sec = 60, int max_missed = 3)
      : heartbeat_timeout_(heartbeat_timeout_sec),
        max_missed_heartbeats_(max_missed) {}

  // Update health record from a received heartbeat.
  void record_heartbeat(int server_id, const ServerHealth &health) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto &record = servers_[server_id];
    bool was_unhealthy =
        !record.is_healthy &&
        record.last_heartbeat !=
            std::chrono::steady_clock::time_point{}; // doesn't this should or?

    record = health;
    record.is_healthy =
        true; // doesn't this invalidate whatever was_healthy check for ? did we
              // ever used this? Why are we overwriting this
    record.missed_heartbeats = 0;
    record.last_heartbeat = std::chrono::steady_clock::now();

    if (was_unhealthy && recovered_cb_) {
      recovered_cb_(server_id, record);
    }
  }

  /// Check all servers for stale heartbeats. Call this periodically.
  void check_health() {
    std::lock_guard<std::mutex> lock(mutex_);
    auto now = std::chrono::steady_clock::now();

    for (auto &[sid, health] : servers_) {
      if (health.last_heartbeat == std::chrono::steady_clock::time_point{})
        continue;

      auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                         now - health.last_heartbeat)
                         .count();

      if (elapsed > heartbeat_timeout_) {
        health.missed_heartbeats++;
        if (health.is_healthy &&
            health.missed_heartbeats >= max_missed_heartbeats_) {
          health.is_healthy = false;
          std::cerr << "Server " << sid << " (" << health.ip
                    << ") marked UNHEALTHY after " << health.missed_heartbeats
                    << " missed heartbeats" << std::endl;
          if (unhealthy_cb_) {
            unhealthy_cb_(sid, health);
          }
        }
      }
    }
  }

  /// Get a snapshot of all tracked servers.
  std::map<int, ServerHealth> get_all_health() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return servers_;
  }

  /// Get count of healthy servers.
  int healthy_count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    int count = 0;
    for (const auto &[_, h] : servers_) {
      if (h.is_healthy)
        count++;
    }
    return count;
  }

  /// Get total tracked servers.
  int total_count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return static_cast<int>(servers_.size());
  }

  /// Register a new server dynamically (runtime registration).
  void register_server(int server_id, const std::string &ip) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (servers_.find(server_id) == servers_.end()) {
      ServerHealth h;
      h.server_id = server_id;
      h.ip = ip;
      h.is_healthy = true;
      h.last_heartbeat = std::chrono::steady_clock::now();
      servers_[server_id] = h;
      std::cout << "Registered server " << server_id << " (" << ip << ")"
                << std::endl;
    }
  }

  /// Remove a server (runtime deregistration).
  void deregister_server(int server_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    servers_.erase(server_id);
    std::cout << "Deregistered server " << server_id << std::endl;
  }

  void set_unhealthy_callback(UnhealthyCallback cb) {
    unhealthy_cb_ = std::move(cb);
  }
  void set_recovered_callback(RecoveredCallback cb) {
    recovered_cb_ = std::move(cb);
  }

private:
  mutable std::mutex mutex_;
  std::map<int, ServerHealth> servers_;
  int heartbeat_timeout_;
  int max_missed_heartbeats_;
  UnhealthyCallback unhealthy_cb_;
  RecoveredCallback recovered_cb_;
};

} // namespace dfg
