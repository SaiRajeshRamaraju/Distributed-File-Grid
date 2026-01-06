#include "./redis_handler.hpp"
#include <cstring>
#include <iostream>
#include <string>
#include <sys/statvfs.h>

// Include the generated version header
#include <version.h>
int main(int argc, char **argv) {

  if (argc > 1) {
    std::string arg = argv[1];
    if (arg == "-h" || arg == "--help") {
      std::cout << "Usage: cluster_server [OPTIONS]" << std::endl;
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
  start_daemon();

  return 0;
}
