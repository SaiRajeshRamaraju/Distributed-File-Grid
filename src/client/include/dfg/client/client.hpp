#pragma once

#include <string>

namespace dfg::client {
int file_delete(std::string server_ip, int server_port, std::string file_name);
int file_download(std::string server_ip, int server_port, std::string file_name,
                  std::string output_path);
int file_upload(const std::string &server_ip, int server_port,
                const std::string &file_path, const std::string &file_name);
int file_list(const std::string &server_ip, int server_port);
int run_client_test(const std::string &server_ip, int server_port);
} // namespace dfg::client
