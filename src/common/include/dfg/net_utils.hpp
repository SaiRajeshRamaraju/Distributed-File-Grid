#pragma once
// Network Utilities
// Non-blocking socket helpers shared across upload/download paths.

#include <arpa/inet.h>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <string>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

namespace dfg::net {

/// Wait without fd_set's descriptor limit. EINTR is not a socket failure.
inline bool wait_for_socket(int fd, short events, int timeout_ms) {
  pollfd descriptor{fd, events, 0};
  int result;
  do {
    result = ::poll(&descriptor, 1, timeout_ms);
  } while (result < 0 && errno == EINTR);
  return result > 0 && !(descriptor.revents & POLLNVAL);
}

/// Drain an entire buffer over a non-blocking socket.
/// Returns true only if all `len` bytes were sent.
inline bool send_all(int sock, const void *buf, size_t len) {
  const uint8_t *p = static_cast<const uint8_t *>(buf);
  size_t sent = 0;
  while (sent < len) {
    ssize_t n = ::send(sock, p + sent, len - sent, MSG_NOSIGNAL);
    if (n > 0) {
      sent += static_cast<size_t>(n);
    } else if (n < 0 && errno == EINTR) {
      continue;
    } else if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      if (!wait_for_socket(sock, POLLOUT, 10000))
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
    } else if (errno == EINTR) {
      continue;
    } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
      if (!wait_for_socket(sock, POLLIN, 10000))
        return false;
    } else {
      return false;
    }
  }
  return true;
}

/// Read a newline-terminated command without consuming subsequent payload
/// bytes. Reject disconnected peers, incomplete lines, and oversized commands.
inline bool recv_line(int sock, std::string &line, size_t max_length = 4096) {
  line.clear();
  while (line.size() < max_length) {
    char byte;
    if (!recv_all(sock, &byte, 1))
      return false;
    if (byte == '\n') {
      if (!line.empty() && line.back() == '\r')
        line.pop_back();
      return true;
    }
    line += byte;
  }
  return false;
}

/// Non-blocking connect with timeout (seconds).
/// Returns socket fd on success, -1 on failure.
inline int connect_with_timeout(const std::string &host, int port,
                                int timeout_sec = 5) {
  if (port < 1 || port > 65535 || timeout_sec <= 0)
    return -1;
  addrinfo hints{};
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  addrinfo *addresses = nullptr;
  const std::string service = std::to_string(port);
  if (::getaddrinfo(host.c_str(), service.c_str(), &hints, &addresses) != 0)
    return -1;
  int connected = -1;
  for (auto *address = addresses; address; address = address->ai_next) {
    int sock = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0);
    if (sock < 0)
      continue;
    int status = ::connect(sock, address->ai_addr, address->ai_addrlen);
    bool ready =
        status == 0 || (errno == EINPROGRESS &&
                        wait_for_socket(sock, POLLOUT, timeout_sec * 1000));
    int error = 0;
    socklen_t size = sizeof(error);
    if (ready && ::getsockopt(sock, SOL_SOCKET, SO_ERROR, &error, &size) == 0 &&
        error == 0) {
      int flags = ::fcntl(sock, F_GETFL, 0);
      timeval timeout{timeout_sec, 0};
      if (flags >= 0 && ::fcntl(sock, F_SETFL, flags & ~O_NONBLOCK) == 0 &&
          ::setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &timeout,
                       sizeof(timeout)) == 0 &&
          ::setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &timeout,
                       sizeof(timeout)) == 0) {
        connected = sock;
        break;
      }
    }
    ::close(sock);
  }
  ::freeaddrinfo(addresses);
  return connected;
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
