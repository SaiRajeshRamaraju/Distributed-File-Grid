#pragma once

#include <dfg/net_utils.hpp>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <poll.h>
#include <stdexcept>
#include <thread>
#include <vector>

// Bind before starting the worker; collect entire streams, including fragmented
// TCP frames. All worker threads are joined before the fixture is destroyed.
class TcpTestServer {
public:
  TcpTestServer() {
    fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd_ < 0) throw std::runtime_error("socket failed");
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    socklen_t len = sizeof(addr);
    if (::bind(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0 ||
        ::listen(fd_, 128) != 0 ||
        ::getsockname(fd_, reinterpret_cast<sockaddr*>(&addr), &len) != 0) {
      ::close(fd_);
      throw std::runtime_error("listener setup failed");
    }
    port_ = ntohs(addr.sin_port);
    worker_ = std::thread([this] { receive(); });
  }

  ~TcpTestServer() {
    running_ = false;
    worker_.join();
    ::close(fd_);
  }

  TcpTestServer(const TcpTestServer&) = delete;
  TcpTestServer& operator=(const TcpTestServer&) = delete;
  int port() const { return port_; }

  bool wait_for_messages(size_t count) {
    std::unique_lock lock(mutex_);
    return ready_.wait_for(lock, std::chrono::seconds(5),
                          [&] { return messages_.size() >= count; });
  }

  std::vector<std::vector<char>> messages() {
    std::lock_guard lock(mutex_);
    return messages_;
  }

private:
  void receive() {
    while (running_) {
      pollfd listener{fd_, POLLIN, 0};
      if (::poll(&listener, 1, 50) <= 0) continue;
      int client = ::accept4(fd_, nullptr, nullptr, SOCK_CLOEXEC);
      if (client < 0) continue;
      timeval timeout{0, 100000};
      ::setsockopt(client, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
      std::vector<char> message;
      char buffer[4096];
      while (running_) {
        ssize_t n = ::recv(client, buffer, sizeof(buffer), 0);
        if (n > 0) message.insert(message.end(), buffer, buffer + n);
        else if (n == 0) break;
        else if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) break;
      }
      ::close(client);
      {
        std::lock_guard lock(mutex_);
        messages_.push_back(std::move(message));
      }
      ready_.notify_all();
    }
  }

  int fd_ = -1;
  int port_ = 0;
  std::atomic<bool> running_{true};
  std::mutex mutex_;
  std::condition_variable ready_;
  std::vector<std::vector<char>> messages_;
  std::thread worker_;
};
