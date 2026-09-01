#pragma once
// ─────────────────── Network Utilities ───────────────────
// Non-blocking socket helpers shared across upload/download paths.
// Extracted from asyc_file_recv_to_chunks.cpp and chunck_read_to_file.cpp
// to eliminate code duplication.

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <string>

#include <arpa/inet.h>
#include <cstring>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

namespace dfg::net {

/// Drain an entire buffer over a non-blocking socket.
/// Returns true only if all `len` bytes were sent.
inline bool send_all(int sock, const void *buf, size_t len) {
  const uint8_t *p = static_cast<const uint8_t *>(buf);
  size_t sent = 0;
  while (sent < len) {
    ssize_t n = ::send(sock, p + sent, len - sent, MSG_NOSIGNAL);
    if (n > 0) {
      sent += static_cast<size_t>(n);
    } else if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      fd_set wfds;
      FD_ZERO(&wfds);
      FD_SET(sock, &wfds);
      struct timeval tv = {10, 0};
      int rc = ::select(sock + 1, nullptr, &wfds, nullptr, &tv);
      if (rc <= 0)
        return false;
    } else {
      return false;
    }
  }
  return true;
}

/// Receive exactly `len` bytes from a non-blocking socket.
/// Returns true only if all bytes were received.
inline bool recv_all(int sock, void *buf, size_t len) {
  uint8_t *p = static_cast<uint8_t *>(buf);
  size_t got = 0;
  while (got < len) {
    ssize_t n = ::recv(sock, p + got, len - got, 0);
    if (n > 0) {
      got += static_cast<size_t>(n);
    } else if (n == 0) {
      return false;
    } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
      fd_set rfds;
      FD_ZERO(&rfds);
      FD_SET(sock, &rfds);
      struct timeval tv = {10, 0};
      int rc = ::select(sock + 1, &rfds, nullptr, nullptr, &tv);
      if (rc <= 0)
        return false;
    } else {
      return false;
    }
  }
  return true;
}

/// Non-blocking connect with timeout (seconds).
/// Returns socket fd on success, -1 on failure.
inline int connect_with_timeout(const std::string &ip, int port,
                                int timeout_sec = 5) {
  int sock = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0);
  if (sock < 0)
    return -1;

  struct sockaddr_in serv_addr{};
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port = htons(port);
  if (::inet_pton(AF_INET, ip.c_str(), &serv_addr.sin_addr) <= 0) {
    struct hostent *he = ::gethostbyname(ip.c_str());
    if (!he || he->h_addrtype != AF_INET) {
      ::close(sock);
      return -1;
    }
    std::memcpy(&serv_addr.sin_addr, he->h_addr_list[0], sizeof(struct in_addr));
  }

  int rc = ::connect(sock, reinterpret_cast<sockaddr *>(&serv_addr),
                     sizeof(serv_addr));
  if (rc < 0 && errno == EINPROGRESS) {
    fd_set wfds;
    FD_ZERO(&wfds);
    FD_SET(sock, &wfds);
    struct timeval tv = {timeout_sec, 0};
    rc = ::select(sock + 1, nullptr, &wfds, nullptr, &tv);
    if (rc <= 0) {
      ::close(sock);
      return -1;
    }
    int err = 0;
    socklen_t len = sizeof(err);
    if (::getsockopt(sock, SOL_SOCKET, SO_ERROR, &err, &len) < 0 || err != 0) {
      ::close(sock);
      return -1;
    }
  } else if (rc < 0) {
    ::close(sock);
    return -1;
  }

  return sock;
}

/// Parse "host:port" into separate ip string and port int.
/// Otherwise , set ip to address and port to default_port.
inline void parse_address(const std::string &address, std::string &ip,
                          int &port, int default_port = 8080) {
  ip = address;
  port = default_port;
  size_t pos = address.find(':');
  if (pos != std::string::npos) {
    ip = address.substr(0, pos);
    try {
      port = std::stoi(address.substr(pos + 1));
    } catch (...) {
      port = default_port;
    }
  }
}

} // namespace dfg::net
