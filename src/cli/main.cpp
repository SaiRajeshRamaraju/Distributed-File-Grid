// ─────────────────── Distributed File Grid — Unified Entry Point
// ─────────────────── Single binary `dfg` with subcommands for all services and
// operations.

#include "../head_server/redis_handler.hpp"
#include <dfg/config_loader.hpp>
#include <dfg/net_utils.hpp>
#include <dfg/version.hpp>

#include <chrono>
#include <fstream>
#include <iostream>
#include <signal.h>
#include <sstream>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

// Forward declarations — linked from other translation units
int run_head_server(int argc, char **argv);

int start_cluster_server(int server_id, const char *ip, int port);

int process_file_upload(const char *filepath, const char *filename);
int process_file_download(const char *filename, const char *output_path);
int check_file_exists(const char *filename);

volatile bool g_running = true;

void signal_handler(int signal) {
  std::cout << "\nReceived signal " << signal << ", shutting down gracefully..."
            << std::endl;
  g_running = false;
}

static void print_usage() {
  std::cout << R"(
╔═══════════════════════════════════════════════════╗
║       Distributed File Grid — Unified CLI         ║
╚═══════════════════════════════════════════════════╝

Usage: dfg <command> [options]

Services:
  head-server       Start the head server (metadata, health, control API)
  cluster-server    Start a cluster server (chunk storage)
  
File Operations:
  upload <path> <name>     Upload a file to distributed storage
  download <name> <path>   Download a file from distributed storage
  list                     List all files in storage
  status                   Show chunk distribution for a file
  
Server Management:
  add-server --host HOST --port PORT    Register a cluster server at runtime
  remove-server --id ID                 Remove a cluster server
  list-servers                          List all cluster servers + health
  
General:
  test                     Run system tests
  -h, --help               Show this help message
  -v, --version            Show version information

Examples:
  dfg head-server
  dfg cluster-server --server-id 1 --port 8080
  dfg upload /path/to/file.txt myfile.txt
  dfg download myfile.txt /path/to/output.txt
  dfg add-server --host 192.168.1.50 --port 8083
  dfg list-servers
)" << std::endl;
}

static int cmd_upload(const std::string &filepath,
                      const std::string &filename) {
  std::cout << "Uploading file: " << filepath << " as " << filename
            << std::endl;
  int result = process_file_upload(filepath.c_str(), filename.c_str());
  if (result == 0) {
    std::cout << "File uploaded successfully!" << std::endl;
  } else {
    std::cout << "File upload failed!" << std::endl;
  }
  return result;
}

static int cmd_download(const std::string &filename,
                        const std::string &output_path) {
  std::cout << "Downloading file: " << filename << " to " << output_path
            << std::endl;
  if (check_file_exists(filename.c_str()) != 1) {
    std::cout << "File not found: " << filename << std::endl;
    return -1;
  }
  int result = process_file_download(filename.c_str(), output_path.c_str());
  if (result == 0) {
    std::cout << "File downloaded successfully!" << std::endl;
  } else {
    std::cout << "File download failed!" << std::endl;
  }
  return result;
}

static int cmd_list_files() {
  auto files = list_all_files();
  if (files.empty()) {
    std::cout << "No files tracked in metadata store." << std::endl;
    return 0;
  }
  std::cout << "Tracked files:" << std::endl;
  for (const auto &file : files) {
    std::cout << "  - " << file << std::endl;
  }
  return 0;
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

  // Extract body from response
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

static int cmd_test() {
  std::cout << "Running system tests..." << std::endl;

  std::string test_file = "/tmp/dfg_test_file.txt";
  std::ofstream file(test_file);
  file << "This is a test file for the distributed storage system.\n";
  for (int i = 0; i < 1000; i++) {
    file << "Line " << i << ": Lorem ipsum dolor sit amet.\n";
  }
  file.close();

  std::cout << "\n=== Testing File Upload ===" << std::endl;
  if (cmd_upload(test_file, "test_file.txt") != 0) {
    std::cout << "Upload test failed!" << std::endl;
    return -1;
  }

  std::cout << "\n=== Testing File Download ===" << std::endl;
  std::string download_path = "/tmp/dfg_downloaded_test.txt";
  if (cmd_download("test_file.txt", download_path) != 0) {
    std::cout << "Download test failed!" << std::endl;
    return -1;
  }

  std::cout << "\n=== Verifying File Integrity ===" << std::endl;
  std::ifstream original(test_file, std::ios::binary);
  std::ifstream downloaded(download_path, std::ios::binary);

  if (original && downloaded) {
    std::string orig_content((std::istreambuf_iterator<char>(original)),
                             std::istreambuf_iterator<char>());
    std::string down_content((std::istreambuf_iterator<char>(downloaded)),
                             std::istreambuf_iterator<char>());

    if (orig_content == down_content) {
      std::cout << "File integrity verified — files match!" << std::endl;
    } else {
      std::cout << "File integrity check failed — files don't match!"
                << std::endl;
      return -1;
    }
  }

  std::cout << "\n=== All Tests Passed! ===" << std::endl;
  return 0;
}

int main(int argc, char *argv[]) {
  signal(SIGINT, signal_handler);
  signal(SIGTERM, signal_handler);

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
    std::cout << "Distributed File Grid version " << APP_VERSION << std::endl;
    return 0;
  }

  // Parse common arguments
  int server_id = 1;
  std::string ip = "0.0.0.0";
  int port = 8080;
  std::string host;
  int target_id = -1;

  for (int i = 2; i < argc; i++) {
    std::string arg = argv[i];
    if (arg == "--server-id" && i + 1 < argc)
      server_id = std::stoi(argv[++i]);
    // pass the ip value only if you want this to listen on specific interface
    // only.
    else if (arg == "--ip" && i + 1 < argc)
      ip = argv[++i];
    else if (arg == "--port" && i + 1 < argc)
      port = std::stoi(argv[++i]);
    else if (arg == "--host" && i + 1 < argc)
      host = argv[++i];
    else if (arg == "--id" && i + 1 < argc)
      target_id = std::stoi(argv[++i]);
  }

  try {
    if (command == "head-server") {
      return run_head_server(argc, argv);
    } else if (command == "cluster-server") {
      return start_cluster_server(server_id, ip.c_str(), port);
    } else if (command == "upload") {
      if (argc < 4) {
        std::cout << "Usage: dfg upload <filepath> <filename>" << std::endl;
        return 1;
      }
      return cmd_upload(argv[2], argv[3]);
    } else if (command == "download") {
      if (argc < 4) {
        std::cout << "Usage: dfg download <filename> <output_path>"
                  << std::endl;
        return 1;
      }
      return cmd_download(argv[2], argv[3]);
    } else if (command == "list") {
      return cmd_list_files();
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
    } else if (command == "test") {
      return cmd_test();
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
