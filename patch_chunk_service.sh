#!/bin/bash
set -e

# Update chunk_service.cpp
sed -i 's/int server_id;\n  std::string server_ip;\n  int port;/int server_id;\n  std::string server_ip;\n  int port;\n  int public_port;/' src/cluster_server/chunk_service.cpp

sed -i 's/ClusterServerService(int id, const std::string &ip, int p)/ClusterServerService(int id, const std::string \&ip, int p, int pp)/' src/cluster_server/chunk_service.cpp

sed -i 's/        server_ip(ip), port(p) {}/        server_ip(ip), port(p), public_port(pp) {}/' src/cluster_server/chunk_service.cpp

sed -i 's/int start_cluster_server(int server_id, const char \*ip, int port) {/int start_cluster_server(int server_id, const char *ip, int port, int public_port) {/' src/cluster_server/chunk_service.cpp

sed -i 's/std::make_unique<ClusterServerService>(server_id, ip, port);/std::make_unique<ClusterServerService>(server_id, ip, port, public_port);/' src/cluster_server/chunk_service.cpp

