#pragma once

#include <atomic>

inline std::atomic<bool> g_is_dead_process{false};

int start_cluster_server(int server_id, const char *ip, int port,
                         int transfer_port, int public_port);
void stop_cluster_server();
