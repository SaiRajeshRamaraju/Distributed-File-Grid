#include "../include/heart_beat_signal.hpp"
#include "../include/version.h"
#include <cstring> // for std::strcmp
#include <iostream>
#include <ifaddrs.h>
#include <netinet/in.h>
#include <arpa/inet.h>

std::string chooseLanAddress() {
    struct ifaddrs *ifaddr, *ifa;
    char host[NI_MAXHOST];

    std::vector<std::string> interfaceNames;
    std::vector<std::string> interfaceIps;

    if (getifaddrs(&ifaddr) == -1) {
        perror("getifaddrs");
        return "";
    }

    for (ifa = ifaddr; ifa != nullptr; ifa = ifa->ifa_next) {
        if (!ifa->ifa_addr)
            continue;

        // Only IPv4
        if (ifa->ifa_addr->sa_family == AF_INET) {
            // Skip loopback
            if (std::string(ifa->ifa_name) == "lo")
                continue;

            if (getnameinfo(
                    ifa->ifa_addr,
                    sizeof(struct sockaddr_in),
                    host,
                    NI_MAXHOST,
                    nullptr,
                    0,
                    NI_NUMERICHOST) == 0) {

                interfaceNames.push_back(ifa->ifa_name);
                interfaceIps.push_back(host);
            }
        }
    }

    freeifaddrs(ifaddr);

    if (interfaceIps.empty()) {
        std::cerr << "No usable network interfaces found\n";
        return "";
    }

    // Show available interfaces
    std::cout << "Available network interfaces:\n";
    for (size_t i = 0; i < interfaceIps.size(); ++i) {
        std::cout << "  " << (i + 1) << ") "
                  << interfaceNames[i]
                  << " -> "
                  << interfaceIps[i] << "\n";
    }

    // In non-interactive mode (Docker), auto-select first interface
    if (!isatty(fileno(stdin))) {
        std::cout << "Non-interactive mode: auto-selecting " << interfaceNames[0] << " -> " << interfaceIps[0] << std::endl;
        return interfaceIps[0];
    }

    // User selection
    size_t choice = 0;
    std::cout << "\nSelect interface number: ";
    std::cin >> choice;

    if (choice < 1 || choice > interfaceIps.size()) {
        std::cerr << "Invalid selection\n";
        return "";
    }

    return interfaceIps[choice - 1];
}



extern "C" {
    int start_cluster_server(int server_id, const char* ip, int port);
}

int main(int argc, char **argv) {
  if (argc > 1) {
    std::string arg = argv[1];

    if (arg == "-h" || arg == "--help") {
      std::cout << "Usage: cluster_server [OPTIONS]\n";
      std::cout << "Options:\n";
      std::cout << "  -h, --help     Show this help message and exit\n";
      std::cout << "  -v, --version  Show program's version number and exit\n";
      std::cout << "  --server-id ID Set the server ID\n";
      std::cout << "  --port PORT    Set the port number\n";
      return 0;
    }
    if (arg == "-v" || arg == "-V" || arg == "--version") {
      std::cout << "Current version: " << APP_VERSION << std::endl;
      return 0;
    }
    
    // Parse command line arguments
    int server_id = 1;
    std::string ip = chooseLanAddress();
    int port = 8080;
    
    for (int i = 1; i < argc; i++) {
      std::string current_arg = argv[i];
      if (current_arg == "--server-id" && i + 1 < argc) {
        server_id = std::stoi(argv[++i]);
      } else if (current_arg == "--port" && i + 1 < argc) {
        port = std::stoi(argv[++i]);
      } else if (current_arg == "--ip" && i + 1 < argc) {
        ip = argv[++i];
      }
    }
    
    // Start cluster server service
    std::cout << "Starting Cluster Server " << server_id << " on " << ip << ":" << port << std::endl;
	std::cout << "Hi, if you started in localhost mode or uses 127.0.0.1. The server can't be accessed by other machines.This can't connect with other machiens" << std::endl;
    std::cout << "Note: Heartbeat functionality requires health checker to be running" << std::endl;
    
    return start_cluster_server(server_id, ip.c_str(), port);
  }
  
  std::cout << "Usage: cluster_server [OPTIONS]" << std::endl;
  std::cout << "Use -h or --help for more information" << std::endl;
  return 0;
}
