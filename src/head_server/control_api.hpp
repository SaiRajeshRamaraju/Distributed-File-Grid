#pragma once
// ─────────────────── HTTP Control API ───────────────────
// Lightweight HTTP API for runtime server management.
// Runs on a separate port (default 9670) alongside the head server.
// Uses raw sockets — no external HTTP library needed.

#include <dfg/server_registry.hpp>
#include <dfg/health_monitor.hpp>
#include <dfg/net_utils.hpp>

#include <string>
#include <thread>
#include <atomic>
#include <sstream>
#include <iostream>
#include <map>
#include <functional>
#include <cstring>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

namespace dfg {

class ControlAPI {
public:
    ControlAPI(ServerRegistry& registry, HealthMonitor& health, int port = 9670)
        : registry_(registry), health_(health), port_(port), running_(false) {}

    ~ControlAPI() { stop(); }

    void start() {
        running_ = true;
        thread_ = std::thread([this]() { run_server(); });
        std::cout << "Control API started on port " << port_ << std::endl;
        std::cout << "  POST   /api/v1/servers/cluster   - Register cluster server" << std::endl;
        std::cout << "  DELETE /api/v1/servers/cluster/ID - Remove cluster server" << std::endl;
        std::cout << "  GET    /api/v1/servers/cluster    - List cluster servers" << std::endl;
        std::cout << "  GET    /api/v1/status             - System status" << std::endl;
    }

    void stop() {
        running_ = false;
        if (thread_.joinable()) {
            // Wake up accept() by connecting to ourselves
            int wake = ::socket(AF_INET, SOCK_STREAM, 0);
            if (wake >= 0) {
                sockaddr_in addr{};
                addr.sin_family = AF_INET;
                addr.sin_port = htons(port_);
                addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
                ::connect(wake, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
                ::close(wake);
            }
            thread_.join();
        }
    }

private:
    void run_server() {
        int lfd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        if (lfd < 0) {
            std::cerr << "ControlAPI: socket failed" << std::endl;
            return;
        }
        int yes = 1;
        setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port_);
        addr.sin_addr.s_addr = htonl(INADDR_ANY);

        if (::bind(lfd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
            std::cerr << "ControlAPI: bind failed on port " << port_ << std::endl;
            ::close(lfd);
            return;
        }
        ::listen(lfd, 16);

        while (running_) {
            sockaddr_in peer{};
            socklen_t plen = sizeof(peer);
            int cfd = ::accept(lfd, reinterpret_cast<sockaddr*>(&peer), &plen);
            if (cfd < 0) continue;
            if (!running_) { ::close(cfd); break; }
            handle_request(cfd);
            ::close(cfd);
        }
        ::close(lfd);
    }

    void handle_request(int cfd) {
        char buf[4096] = {};
        ssize_t n = ::recv(cfd, buf, sizeof(buf) - 1, 0);
        if (n <= 0) return;

        std::string request(buf, n);
        std::string method, path;
        std::istringstream iss(request);
        iss >> method >> path;

        std::string body;
        auto body_start = request.find("\r\n\r\n");
        if (body_start != std::string::npos) {
            body = request.substr(body_start + 4);
        }

        std::string response;

        if (method == "GET" && path == "/api/v1/servers/cluster") {
            response = handle_list_servers();
        } else if (method == "POST" && path == "/api/v1/servers/cluster") {
            response = handle_add_server(body);
        } else if (method == "DELETE" && path.find("/api/v1/servers/cluster/") == 0) {
            std::string id_str = path.substr(23);
            response = handle_remove_server(id_str);
        } else if (method == "GET" && path == "/api/v1/status") {
            response = handle_status();
        } else if (method == "POST" && path == "/api/v1/servers/cluster/register") {
            response = handle_self_register(body);
        } else {
            response = make_response(404, "{\"error\":\"Not found\"}");
        }

        ::send(cfd, response.data(), response.size(), 0);
    }

    std::string handle_list_servers() {
        auto servers = registry_.get_all();
        auto health_map = health_.get_all_health();
        
        std::ostringstream json;
        json << "{\"servers\":[";
        bool first = true;
        for (const auto& s : servers) {
            if (!first) json << ",";
            first = false;
            json << "{\"id\":" << s.id
                 << ",\"host\":\"" << s.host << "\""
                 << ",\"port\":" << s.port
                 << ",\"status\":\"" << s.status << "\"";
            
            auto hit = health_map.find(s.id);
            if (hit != health_map.end()) {
                json << ",\"cpu\":" << hit->second.cpu_usage
                     << ",\"ram\":" << hit->second.ram_usage
                     << ",\"disk\":" << hit->second.disk_usage
                     << ",\"healthy\":" << (hit->second.is_healthy ? "true" : "false");
            }
            json << "}";
        }
        json << "],\"total\":" << servers.size() << "}";
        return make_response(200, json.str());
    }

    std::string handle_add_server(const std::string& body) {
        // Parse simple JSON: {"host":"x.x.x.x","port":8080}
        std::string host = extract_json_string(body, "host");
        int port = extract_json_int(body, "port");
        
        if (host.empty() || port <= 0) {
            return make_response(400, "{\"error\":\"Missing host or port\"}");
        }
        
        int id = registry_.add_server(host, port);
        health_.register_server(id, host + ":" + std::to_string(port));
        
        std::ostringstream json;
        json << "{\"id\":" << id << ",\"host\":\"" << host << "\",\"port\":" << port << ",\"status\":\"active\"}";
        return make_response(201, json.str());
    }

    std::string handle_remove_server(const std::string& id_str) {
        try {
            int id = std::stoi(id_str);
            if (registry_.remove_server(id)) {
                health_.deregister_server(id);
                return make_response(200, "{\"removed\":true,\"id\":" + std::to_string(id) + "}");
            }
            return make_response(404, "{\"error\":\"Server not found\"}");
        } catch (...) {
            return make_response(400, "{\"error\":\"Invalid server ID\"}");
        }
    }

    std::string handle_self_register(const std::string& body) {
        // Cluster server self-registers: {"server_id":1,"host":"x.x.x.x","port":8080}
        int server_id = extract_json_int(body, "server_id");
        std::string host = extract_json_string(body, "host");
        int port = extract_json_int(body, "port");

        if (host.empty() || port <= 0) {
            return make_response(400, "{\"error\":\"Missing host or port\"}");
        }

        // Use provided server_id if valid, otherwise auto-assign
        if (server_id > 0) {
            registry_.set_next_id(std::max(server_id + 1, registry_.count() + 1));
        }
        int id = registry_.add_server(host, port);
        health_.register_server(id, host + ":" + std::to_string(port));

        std::ostringstream json;
        json << "{\"id\":" << id << ",\"registered\":true}";
        return make_response(200, json.str());
    }

    std::string handle_status() {
        auto servers = registry_.get_all();
        auto health_map = health_.get_all_health();
        
        int healthy = 0, unhealthy = 0;
        for (const auto& [_, h] : health_map) {
            if (h.is_healthy) healthy++;
            else unhealthy++;
        }
        
        std::ostringstream json;
        json << "{\"total_servers\":" << servers.size()
             << ",\"healthy\":" << healthy
             << ",\"unhealthy\":" << unhealthy
             << ",\"status\":\"" << (unhealthy == 0 ? "OK" : "DEGRADED") << "\""
             << "}";
        return make_response(200, json.str());
    }

    static std::string make_response(int code, const std::string& body) {
        std::string status_text;
        switch (code) {
            case 200: status_text = "OK"; break;
            case 201: status_text = "Created"; break;
            case 400: status_text = "Bad Request"; break;
            case 404: status_text = "Not Found"; break;
            default: status_text = "Error"; break;
        }
        std::ostringstream oss;
        oss << "HTTP/1.1 " << code << " " << status_text << "\r\n"
            << "Content-Type: application/json\r\n"
            << "Content-Length: " << body.size() << "\r\n"
            << "Connection: close\r\n"
            << "\r\n"
            << body;
        return oss.str();
    }

    // Simple JSON extractors (no external dependency)
    static std::string extract_json_string(const std::string& json, const std::string& key) {
        std::string search = "\"" + key + "\"";
        auto pos = json.find(search);
        if (pos == std::string::npos) return "";
        pos = json.find(':', pos);
        if (pos == std::string::npos) return "";
        pos = json.find('"', pos);
        if (pos == std::string::npos) return "";
        auto end = json.find('"', pos + 1);
        if (end == std::string::npos) return "";
        return json.substr(pos + 1, end - pos - 1);
    }

    static int extract_json_int(const std::string& json, const std::string& key) {
        std::string search = "\"" + key + "\"";
        auto pos = json.find(search);
        if (pos == std::string::npos) return 0;
        pos = json.find(':', pos);
        if (pos == std::string::npos) return 0;
        pos++;
        while (pos < json.size() && (json[pos] == ' ' || json[pos] == '\t')) pos++;
        try { return std::stoi(json.substr(pos)); } catch (...) { return 0; }
    }

    ServerRegistry& registry_;
    HealthMonitor& health_;
    int port_;
    std::atomic<bool> running_;
    std::thread thread_;
};

} // namespace dfg
