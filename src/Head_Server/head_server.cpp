#include "./redis_handler.hpp"
#include "./head_metrics.hpp"
#include <cstring>
#include <iostream>
#include <string>
#include <sys/statvfs.h>
#include <thread>
#include <chrono>

// Include the generated version header
#include "../include/version.h"

// Global metrics instance for the head server
static std::unique_ptr<HeadServerMetrics> g_head_metrics;

HeadServerMetrics* get_head_metrics() { return g_head_metrics.get(); }

int main(int argc, char **argv) {
  if (argc > 1) {
    std::string arg = argv[1];
    if (arg == "-h" || arg == "--help") {
      std::cout << "Usage: head_server [OPTIONS]" << std::endl;
      std::cout << "Options:" << std::endl;
      std::cout << "  -h, --help  Show this help message and exit" << std::endl;
      std::cout << "  -v, --version  Show program's version number and exit"
                << std::endl;
      std::cout << "  -p, --port  Set the port number" << std::endl;
      return 0;
    }
    if (arg == "-v" || arg == "-V" || arg == "--version") {
      std::cout << "Version: " << APP_VERSION_MAJOR << "." << APP_VERSION_MINOR
                << "." << APP_VERSION_PATCH << std::endl;
      return 0;
    }
  }

  // Initialize Prometheus metrics exporter on port 9095
  try {
      g_head_metrics = std::make_unique<HeadServerMetrics>("0.0.0.0:9095");
      std::cout << "Head Server Prometheus metrics available on :9095/metrics" << std::endl;
  } catch (const std::exception& e) {
      std::cerr << "Warning: Could not start metrics exporter: " << e.what() << std::endl;
  }

  start_daemon();

  // Keep the process running
  while (true) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  return 0;
}
