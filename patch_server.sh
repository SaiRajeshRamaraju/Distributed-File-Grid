#!/bin/bash
set -e

# Update cluster_server.cpp
sed -i 's/int start_cluster_server(int server_id, const char \*ip, int port);/int start_cluster_server(int server_id, const char *ip, int port, int public_port);/' src/cluster_server/cluster_server.cpp

sed -i 's/int port = cfg.get_int("server.port", 8080);/int port = cfg.get_int("server.port", 8080);\n  int public_port = cfg.get_int("server.public_port", port + 200);/' src/cluster_server/cluster_server.cpp

sed -i 's/else if (current_arg == "--port" && i + 1 < argc)\n      port = std::stoi(argv\[++i\]);/else if (current_arg == "--port" && i + 1 < argc)\n      port = std::stoi(argv[++i]);\n    else if (current_arg == "--public-port" && i + 1 < argc)\n      public_port = std::stoi(argv[++i]);/' src/cluster_server/cluster_server.cpp

sed -i 's/std::cout << "  --port PORT      Set the port number" << std::endl;/std::cout << "  --port PORT      Set the port number" << std::endl;\n      std::cout << "  --public-port PORT Set public port" << std::endl;/' src/cluster_server/cluster_server.cpp

sed -i 's/return start_cluster_server(server_id, ip.c_str(), port);/return start_cluster_server(server_id, ip.c_str(), port, public_port);/' src/cluster_server/cluster_server.cpp


