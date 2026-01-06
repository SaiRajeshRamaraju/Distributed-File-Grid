#include "include/version.h"
#include "Head_Server/redis_handler.hpp"
#include <iostream>
#include <fstream>
#include <string>
#include <thread>
#include <vector>
#include <chrono>
#include <signal.h>

// Forward declarations for service functions
extern "C" {
    int start_cluster_server(int server_id, const char* ip, int port);
    void stop_cluster_server();
    int start_health_checker();
    void stop_health_checker();
    int process_file_upload(const char* filepath, const char* filename);
    int process_file_download(const char* filename, const char* output_path);
    int check_file_exists(const char* filename);
}

// Global flag for graceful shutdown
volatile bool g_running = true;

void signal_handler(int signal) {
    std::cout << "\nReceived signal " << signal << ", shutting down gracefully..." << std::endl;
    g_running = false;
}

void print_usage() {
    std::cout << "Distributed File Grid - A fault-tolerant distributed storage system\n\n";
    std::cout << "Usage: ./main [SERVICE] [OPTIONS]\n\n";
    std::cout << "Services:\n";
    std::cout << "  head-server     Start the head server (metadata management)\n";
    std::cout << "  cluster-server  Start a cluster server (chunk storage)\n";
    std::cout << "  health-checker  Start the health monitoring service\n";
    std::cout << "  upload          Upload a file to the distributed storage\n";
    std::cout << "  download        Download a file from the distributed storage\n";
    std::cout << "  list            List files in the distributed storage\n";
    std::cout << "  test            Run system tests\n\n";
    std::cout << "Options:\n";
    std::cout << "  -h, --help      Show this help message\n";
    std::cout << "  -v, --version   Show version information\n";
    std::cout << "  --server-id ID  Set server ID (for cluster-server)\n";
    std::cout << "  --port PORT     Set port number\n";
    std::cout << "  --ip IP         Set IP address\n\n";
    std::cout << "Examples:\n";
    std::cout << "  ./main head-server\n";
    std::cout << "  ./main cluster-server --server-id 1 --port 8080\n";
    std::cout << "  ./main health-checker\n";
    std::cout << "  ./main upload /path/to/file.txt myfile.txt\n";
    std::cout << "  ./main download myfile.txt /path/to/output.txt\n";
}

int run_head_server() {
    std::cout << "Starting Head Server..." << std::endl;
    
    // In a real implementation, this would start the HTTP server
    // For now, just keep the process running
    while (g_running) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    
    return 0;
}

int run_cluster_server(int server_id, const std::string& ip, int port) {
    std::cout << "Starting Cluster Server " << server_id << " on " << ip << ":" << port << std::endl;
    
    return start_cluster_server(server_id, ip.c_str(), port);
}

int run_health_checker() {
    std::cout << "Starting Health Checker..." << std::endl;
    
    return start_health_checker();
}

int upload_file(const std::string& filepath, const std::string& filename) {
    std::cout << "Uploading file: " << filepath << " as " << filename << std::endl;
    
    int result = process_file_upload(filepath.c_str(), filename.c_str());
    if (result == 0) {
        std::cout << "File uploaded successfully!" << std::endl;
    } else {
        std::cout << "File upload failed!" << std::endl;
    }
    
    return result;
}

int download_file(const std::string& filename, const std::string& output_path) {
    std::cout << "Downloading file: " << filename << " to " << output_path << std::endl;
    
    // Check if file exists first
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

int list_files_command() {
    auto files = list_all_files();
    if (files.empty()) {
        std::cout << "No files tracked in metadata store." << std::endl;
        return 0;
    }

    std::cout << "Tracked files:" << std::endl;
    for (const auto& file : files) {
        std::cout << "  - " << file << std::endl;
    }
    return 0;
}

int run_tests() {
    std::cout << "Running system tests..." << std::endl;
    
    // Create a test file
    std::string test_file = "/tmp/test_file.txt";
    std::ofstream file(test_file);
    file << "This is a test file for the distributed storage system.\n";
    file << "It contains some sample data to test chunking and reconstruction.\n";
    for (int i = 0; i < 1000; i++) {
        file << "Line " << i << ": Lorem ipsum dolor sit amet, consectetur adipiscing elit.\n";
    }
    file.close();
    
    std::cout << "Created test file: " << test_file << std::endl;
    
    // Test upload
    std::cout << "\n=== Testing File Upload ===" << std::endl;
    if (upload_file(test_file, "test_file.txt") != 0) {
        std::cout << "Upload test failed!" << std::endl;
        return -1;
    }
    
    // Test download
    std::cout << "\n=== Testing File Download ===" << std::endl;
    std::string download_path = "/tmp/downloaded_test_file.txt";
    if (download_file("test_file.txt", download_path) != 0) {
        std::cout << "Download test failed!" << std::endl;
        return -1;
    }
    
    // Verify file integrity
    std::cout << "\n=== Verifying File Integrity ===" << std::endl;
    std::ifstream original(test_file, std::ios::binary);
    std::ifstream downloaded(download_path, std::ios::binary);
    
    if (original && downloaded) {
        std::string orig_content((std::istreambuf_iterator<char>(original)),
                                std::istreambuf_iterator<char>());
        std::string down_content((std::istreambuf_iterator<char>(downloaded)),
                                std::istreambuf_iterator<char>());
        
        if (orig_content == down_content) {
            std::cout << "File integrity verified - files match!" << std::endl;
        } else {
            std::cout << "File integrity check failed - files don't match!" << std::endl;
            return -1;
        }
    } else {
        std::cout << "Could not open files for integrity check!" << std::endl;
        return -1;
    }
    
    std::cout << "\n=== All Tests Passed! ===" << std::endl;
    return 0;
}

int main(int argc, char* argv[]) {
    // Set up signal handlers for graceful shutdown
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
    
    // Parse additional arguments
    int server_id = 1;
    std::string ip = "127.0.0.1";
    int port = 8080;
    
    for (int i = 2; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--server-id" && i + 1 < argc) {
            server_id = std::stoi(argv[++i]);
        } else if (arg == "--ip" && i + 1 < argc) {
            ip = argv[++i];
        } else if (arg == "--port" && i + 1 < argc) {
            port = std::stoi(argv[++i]);
        }
    }
    
    try {
        if (command == "head-server") {
            return run_head_server();
        } else if (command == "cluster-server") {
            return run_cluster_server(server_id, ip, port);
        } else if (command == "health-checker") {
            return run_health_checker();
        } else if (command == "upload") {
            if (argc < 4) {
                std::cout << "Usage: ./main upload <filepath> <filename>" << std::endl;
                return 1;
            }
            return upload_file(argv[2], argv[3]);
        } else if (command == "download") {
            if (argc < 4) {
                std::cout << "Usage: ./main download <filename> <output_path>" << std::endl;
                return 1;
            }
            return download_file(argv[2], argv[3]);
        } else if (command == "list") {
            return list_files_command();
        } else if (command == "test") {
            return run_tests();
        } else {
            std::cout << "Unknown command: " << command << std::endl;
            print_usage();
            return 1;
        }
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}
