#include "client_request.hpp"

#include "file_download.hpp"
#include "file_upload.hpp"

#include <array>
#include <dfg/metadata/store.hpp>
#include <dfg/net_utils.hpp>
#include <dfg/protocol.hpp>
#include <dfg/unique_fd.hpp>
#include <functional>
#include <iostream>
#include <mutex>
#include <sstream>
#include <stdexcept>

namespace {
// Fixed-size locks prevent concurrent upload/delete operations on the same
// remote name without retaining a mutex for every name ever requested.
std::mutex &file_lock(const std::string &filename) {
  static std::array<std::mutex, 64> locks;
  return locks[std::hash<std::string>{}(filename) % locks.size()];
}
} // namespace

void handle_client_request(int socket) {
  dfg::UniqueFd connection(socket);
  try {
    std::string request;
    if (!dfg::net::recv_line(socket, request))
      return;
    std::istringstream fields(request);
    std::string command, filename, extra;
    fields >> command;
    if (command == "LIST" && !(fields >> extra)) {
      std::string response;
      for (const auto &file : list_all_files())
        response += file + "\n";
      response += "EOF\n";
      dfg::net::send_all(socket, response.data(), response.size());
      return;
    }
    if (!(fields >> filename) || !dfg::protocol::valid_name(filename))
      throw std::invalid_argument("Invalid command or remote filename");
    std::lock_guard lock(file_lock(filename));
    if (command == "UPLOAD") {
      handle_client_upload(socket, request);
    } else if (fields >> extra) {
      throw std::invalid_argument("Unexpected command arguments");
    } else if (command == "DOWNLOAD") {
      handle_client_download(socket, filename);
    } else if (command == "DELETE") {
      handle_client_delete(socket, filename);
    } else {
      throw std::invalid_argument("Unknown client command");
    }
  } catch (const std::exception &error) {
    std::cerr << "Client request failed: " << error.what() << std::endl;
    const std::string response = "ERROR: " + std::string(error.what()) + "\n";
    dfg::net::send_all(socket, response.data(), response.size());
  }
}
