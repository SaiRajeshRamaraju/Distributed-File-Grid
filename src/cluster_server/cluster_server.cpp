// ─────────────────── Cluster Server Entry Point ───────────────────
// Standalone entry point for the cluster server binary.
// On startup, self-registers with the head server's control API.

#include <dfg/heart_beat_signal.hpp>
#include <dfg/version.hpp>
#include <dfg/config_loader.hpp>
#include <dfg/net_utils.hpp>

#include <cstring>
#include <iostream>
#include <sstream>
#include <thread>
#include <chrono>
#include <ifaddrs.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

// Forward declaration from async_file_recv.cpp
extern "C" int start_cluster_server(int server_id, const char* ip, int port);

static std::string chooseLanAddress() {
    struct ifaddrs *ifaddr, *ifa;
    char host[NI_MAXHOST];

    std::vector<std::string> interfaceNames;
    std::vector<std::string> interfaceIps;

    if (getifaddrs(&ifaddr) == -1) {
        perror("getifaddrs");
        return "";
    }

    for (ifa = ifaddr; ifa != nullptr; ifa = ifa->ifa_next) {
        if (!ifa->ifa_addr) continue;
        if (ifa->ifa_addr->sa_family == AF_INET) {
            if (std::string(ifa->ifa_name) == "lo") continue;
            if (getnameinfo(ifa->ifa_addr, sizeof(struct sockaddr_in),
                            host, NI_MAXHOST, nullptr, 0, NI_NUMERICHOST) == 0) {
                interfaceNames.push_back(ifa->ifa_name);
                interfaceIps.push_back(host);
            }
        }
    }
    freeifaddrs(ifaddr);

    if (interfaceIps.empty()) {
        std::cerr << "No usable network interfaces found" << std::endl;
        return "";
    }

    std::cout << "Available network interfaces:" << std::endl;
    for (size_t i = 0; i < interfaceIps.size(); ++i) {
        std::cout << "  " << (i + 1) << ") " << interfaceNames[i] << " -> " << interfaceIps[i] << std::endl;
    }

    if (!isatty(fileno(stdin))) {
        std::cout << "Non-interactive mode: auto-selecting " << interfaceNames[0] << " -> " << interfaceIps[0] << std::endl;
        return interfaceIps[0];
    }

    size_t choice = 0;
    std::cout << "\nSelect interface number: ";
    std::cin >> choice;
    if (choice < 1 || choice > interfaceIps.size()) {
        std::cerr << "Invalid selection" << std::endl;
        return "";
    }
    return interfaceIps[choice - 1];
}

/// Self-register with the head server's control API.
static bool self_register_with_head(const std::string& head_host, int head_control_port,
                                     int server_id, const std::string& ip, int port) {
    int sock = dfg::net::connect_with_timeout(head_host, head_control_port, 5);
    if (sock < 0) {
        std::cerr << "Could not connect to head server control API at " 
                  << head_host << ":" << head_control_port << std::endl;
        return false;
    }

    std::ostringstream body;
    body << "{\"server_id\":" << server_id << ",\"host\":\"" << ip << "\",\"port\":" << port << "}";
    std::string body_str = body.str();

    std::ostringstream req;
    req << "POST /api/v1/servers/cluster/register HTTP/1.1\r\n"
        << "Host: " << head_host << "\r\n"
        << "Content-Type: application/json\r\n"
        << "Content-Length: " << body_str.size() << "\r\n"
        << "Connection: close\r\n"
        << "\r\n"
        << body_str;
    
    std::string request = req.str();
    if (!dfg::net::send_all(sock, request.data(), request.size())) {
        ::close(sock);
        return false;
    }

    // Read response
    char resp[1024] = {};
    ::recv(sock, resp, sizeof(resp) - 1, 0);
    ::close(sock);

    std::string response(resp);
    if (response.find("200") != std::string::npos || response.find("201") != std::string::npos) {
        std::cout << "✅ Self-registered with head server at " << head_host << ":" << head_control_port << std::endl;
        return true;
    }
    
    std::cerr << "Self-registration failed: " << response.substr(0, 80) << std::endl;
    return false;
}

int main(int argc, char **argv) {
    if (argc > 1) {
        std::string arg = argv[1];
        if (arg == "-h" || arg == "--help") {
            std::cout << "Usage: cluster_server [OPTIONS]" << std::endl;
            std::cout << "Options:" << std::endl;
            std::cout << "  -h, --help       Show this help message" << std::endl;
            std::cout << "  -v, --version    Show version" << std::endl;
            std::cout << "  --server-id ID   Set the server ID" << std::endl;
            std::cout << "  --port PORT      Set the port number" << std::endl;
            std::cout << "  --ip IP          Set IP address" << std::endl;
            std::cout << "  --no-register    Skip self-registration with head server" << std::endl;
            return 0;
        }
        if (arg == "-v" || arg == "-V" || arg == "--version") {
            std::cout << "Version: " << APP_VERSION << std::endl;
            return 0;
        }
    }
    
    // Load configuration
    auto& cfg = cluster_server_config();

    int server_id = cfg.get_int("server.id", 1);
    std::string ip = cfg.get_string("server.host", "");
    int port = cfg.get_int("server.port", 8080);
    bool should_register = true;
    
    for (int i = 1; i < argc; i++) {
        std::string current_arg = argv[i];
        if (current_arg == "--server-id" && i + 1 < argc)
            server_id = std::stoi(argv[++i]);
        else if (current_arg == "--port" && i + 1 < argc)
            port = std::stoi(argv[++i]);
        else if (current_arg == "--ip" && i + 1 < argc)
            ip = argv[++i];
        else if (current_arg == "--no-register")
            should_register = false;
    }

    if (ip.empty() || ip == "0.0.0.0") {
        ip = chooseLanAddress();
    }
    
    // Print startup banner
    std::cout << std::endl;
    std::cout << "╔═══════════════════════════════════════════════════╗" << std::endl;
    std::cout << "║     Distributed File Grid - Cluster Server        ║" << std::endl;
    std::cout << "║                  Version " << APP_VERSION << "                    ║" << std::endl;
    std::cout << "╚═══════════════════════════════════════════════════╝" << std::endl;
    std::cout << std::endl;
    if (cfg.is_loaded()) {
        std::cout << "  Config Source:      " << cfg.filepath() << std::endl;
    } else {
        std::cout << "  \033[33mConfig Source:      DEFAULTS (no config file loaded)\033[0m" << std::endl;
    }
    std::cout << "  Server ID:          " << server_id << std::endl;
    std::cout << "  Address:            " << ip << ":" << port << std::endl;
    std::cout << "  Head Server:        " << cfg.get_string("head_server.host", "127.0.0.1")
              << ":" << cfg.get_int("head_server.port", 9669) << std::endl;
    std::cout << "  Heartbeat Target:   " << cfg.get_string("heartbeat.target_host", "127.0.0.1")
              << ":" << cfg.get_int("heartbeat.target_port", 9000) << std::endl;
    std::cout << std::endl;

    // Self-register with head server
    if (should_register) {
        std::string head_host = cfg.get_string("head_server.host", "127.0.0.1");
        int head_control_port = cfg.get_int("head_server.control_port", 9670);
        
        // Try registration in background (don't block startup)
        std::thread reg_thread([head_host, head_control_port, server_id, ip, port]() {
            // Retry a few times in case head server isn't up yet
            for (int attempt = 0; attempt < 5; attempt++) {
                if (self_register_with_head(head_host, head_control_port, server_id, ip, port)) {
                    return;
                }
                std::cerr << "Registration attempt " << (attempt + 1) << "/5 failed, retrying in 5s..." << std::endl;
                std::this_thread::sleep_for(std::chrono::seconds(5));
            }
            std::cerr << "\033[33mWarning: Could not self-register with head server. Continuing anyway.\033[0m" << std::endl;
        });
        reg_thread.detach();
    }
    
    return start_cluster_server(server_id, ip.c_str(), port);
}
