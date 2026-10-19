#pragma once

#include <string>

void handle_client_download(int fd, const std::string &filename);
void handle_client_delete(int fd, const std::string &filename);
bool replicate_chunk_to_server(const std::string &source_server, int chunk_id,
                               const std::string &target_server,
                               const std::string &filename,
                               std::string *out_file_path = nullptr,
                               const std::string &source_file_path = "");
