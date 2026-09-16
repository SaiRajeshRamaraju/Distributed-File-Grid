// ─────────────────── Distributed File Grid — Server Entry Point ───────────────────
// Server binary `dfg` for all cluster services and runtime server management.

#include "../head_server/redis_handler.hpp"
#include <dfg/config_loader.hpp>
#include <dfg/net_utils.hpp>
#include <dfg/version.hpp>

#include <iostream>
#include <signal.h>
#include <sstream>
#include <string>
#include <unistd.h>
#include <vector>

// Forward declarations of server runners
int run_head_server(int argc, char **argv);
int run_cluster_server(int argc, char **argv);
int run_health_checker(int argc, char **argv);
int run_zk_monitor(int argc, char **argv);

static volatile bool g_server_running = true;

static void server_signal_handler(int signal) {
  std::cout << "\nReceived signal " << signal << ", shutting down gracefully..."
            << std::endl;
  g_server_running = false;
}

static void print_usage() {
  std::cout << R"(
╔═══════════════════════════════════════════════════╗
║       Distributed File Grid — Server CLI (dfg)    ║
╚═══════════════════════════════════════════════════╝

Usage: dfg <service|command> [options]

Services:
  head-server, head          Start the Head Server (metadata, health, control API)
  cluster-server, cluster    Start a Cluster Storage Server (chunk storage)
  health-checker, health     Start the Health Checker daemon
  zk-monitor, zk             Start the ZooKeeper Monitor daemon

Cluster Management:
  add-server --host HOST --port PORT    Register a cluster server at runtime
  remove-server --id ID                 Remove a cluster server
  list-servers                          List all cluster servers + health

General:
  -h, --help               Show this help message
  -v, --version            Show version information

Examples:
  dfg head-server
  dfg cluster-server --server-id 1 --port 8080
  dfg health-checker
  dfg zk-monitor
  dfg add-server --host 192.168.1.50 --port 8083
  dfg list-servers

Note: For client file operations (upload, download, list, test), use the dedicated 'client' binary:
  client upload <path> [name]
  client download <name> [output_path]
  client list
  client test
)" << std::endl;
}

// ── Runtime server management via HTTP to head server's control API ──
static int cmd_add_server(const std::string &host, int port) {
  auto &cfg = head_server_config();
  std::string head_host = cfg.get_string("head_server.host", "127.0.0.1");
  int control_port = cfg.get_int("head_server.control_port", 9670);

  int sock = dfg::net::connect_with_timeout(head_host, control_port, 5);
  if (sock < 0) {
    std::cerr << "Cannot connect to head server control API at " << head_host
              << ":" << control_port << std::endl;
    return 1;
  }

  std::ostringstream body;
  body << "{\"host\":\"" << host << "\",\"port\":" << port << "}";
  std::string body_str = body.str();

  std::ostringstream req;
  req << "POST /api/v1/servers/cluster HTTP/1.1\r\n"
      << "Host: " << head_host << "\r\n"
      << "Content-Type: application/json\r\n"
      << "Content-Length: " << body_str.size() << "\r\n"
      << "Connection: close\r\n\r\n"
      << body_str;

  std::string request = req.str();
  dfg::net::send_all(sock, request.data(), request.size());

  char resp[2048] = {};
  ::recv(sock, resp, sizeof(resp) - 1, 0);
  ::close(sock);

  std::string response(resp);
  auto body_start = response.find("\r\n\r\n");
  if (body_start != std::string::npos) {
    std::cout << response.substr(body_start + 4) << std::endl;
  }
  return 0;
}

static int cmd_remove_server(int id) {
  auto &cfg = head_server_config();
  std::string head_host = cfg.get_string("head_server.host", "127.0.0.1");
  int control_port = cfg.get_int("head_server.control_port", 9670);

  int sock = dfg::net::connect_with_timeout(head_host, control_port, 5);
  if (sock < 0) {
    std::cerr << "Cannot connect to head server control API" << std::endl;
    return 1;
  }

  std::ostringstream req;
  req << "DELETE /api/v1/servers/cluster/" << id << " HTTP/1.1\r\n"
      << "Host: " << head_host << "\r\n"
      << "Connection: close\r\n\r\n";

  std::string request = req.str();
  dfg::net::send_all(sock, request.data(), request.size());

  char resp[2048] = {};
  ::recv(sock, resp, sizeof(resp) - 1, 0);
  ::close(sock);

  std::string response(resp);
  auto body_start = response.find("\r\n\r\n");
  if (body_start != std::string::npos) {
    std::cout << response.substr(body_start + 4) << std::endl;
  }
  return 0;
}

static int cmd_list_servers() {
  auto &cfg = head_server_config();
  std::string head_host = cfg.get_string("head_server.host", "127.0.0.1");
  int control_port = cfg.get_int("head_server.control_port", 9670);

  int sock = dfg::net::connect_with_timeout(head_host, control_port, 5);
  if (sock < 0) {
    std::cerr << "Cannot connect to head server control API at " << head_host
              << ":" << control_port << std::endl;
    return 1;
  }

  std::ostringstream req;
  req << "GET /api/v1/servers/cluster HTTP/1.1\r\n"
      << "Host: " << head_host << "\r\n"
      << "Connection: close\r\n\r\n";

  std::string request = req.str();
  dfg::net::send_all(sock, request.data(), request.size());

  char resp[8192] = {};
  ::recv(sock, resp, sizeof(resp) - 1, 0);
  ::close(sock);

  std::string response(resp);
  auto body_start = response.find("\r\n\r\n");
  if (body_start != std::string::npos) {
    std::cout << response.substr(body_start + 4) << std::endl;
  }
  return 0;
}

int main(int argc, char *argv[]) {
  signal(SIGINT, server_signal_handler);
  signal(SIGTERM, server_signal_handler);

  if (argc < 2) {
    print_usage();
    return 1;
  }

  std::string command = argv[1];

  if (command == "-h" || command == "--help") {
    print_usage();
    return 0;
  }
  if (command == "-v" || command == "--version") {
    std::cout << "Distributed File Grid Server version " << APP_VERSION << std::endl;
    return 0;
  }

  // File operation redirection
  if (command == "upload" || command == "download" || command == "list" || command == "test") {
    std::cerr << "Error: File operations have been moved to the dedicated 'client' binary.\n";
    std::cerr << "Please use the 'client' executable instead:\n";
    std::cerr << "  client " << command << " [options]\n";
    return 1;
  }

  // Parse common arguments for cluster management
  std::string host;
  int target_id = -1;
  int port = 8080;

  for (int i = 2; i < argc; i++) {
    std::string arg = argv[i];
    if (arg == "--host" && i + 1 < argc)
      host = argv[++i];
    else if (arg == "--id" && i + 1 < argc)
      target_id = std::stoi(argv[++i]);
    else if (arg == "--port" && i + 1 < argc)
      port = std::stoi(argv[++i]);
  }

  try {
    if (command == "head-server" || command == "head") {
      return run_head_server(argc - 1, argv + 1);
    } else if (command == "cluster-server" || command == "cluster") {
      return run_cluster_server(argc - 1, argv + 1);
    } else if (command == "health-checker" || command == "health") {
      return run_health_checker(argc - 1, argv + 1);
    } else if (command == "zk-monitor" || command == "zk") {
      return run_zk_monitor(argc - 1, argv + 1);
    } else if (command == "add-server") {
      if (host.empty()) {
        std::cout << "Usage: dfg add-server --host HOST --port PORT"
                  << std::endl;
        return 1;
      }
      return cmd_add_server(host, port);
    } else if (command == "remove-server") {
      if (target_id < 0) {
        std::cout << "Usage: dfg remove-server --id ID" << std::endl;
        return 1;
      }
      return cmd_remove_server(target_id);
    } else if (command == "list-servers") {
      return cmd_list_servers();
    } else {
      std::cout << "Unknown command: " << command << std::endl;
      print_usage();
      return 1;
    }
  } catch (const std::exception &e) {
    std::cerr << "Error: " << e.what() << std::endl;
    return 1;
  }

  return 0;
}
