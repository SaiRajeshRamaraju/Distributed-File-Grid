#pragma once
#include "dfg/system_info.hpp"
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/timerfd.h>
#include <unistd.h>

#include <chrono>
#include <coroutine>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>

#include "heartbeat.pb.h"
#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/util/time_util.h>

namespace async_hb {

// Forward declare Reactor here for promise_type
struct Reactor;

// Coroutine task with promise_type
struct task {
  struct promise_type {
    Reactor *reactor{nullptr};
    std::coroutine_handle<> continuation;

    task get_return_object();
    std::suspend_always initial_suspend() noexcept { return {}; }
    struct final_awaitable {
      bool await_ready() noexcept { return false; }
      void await_suspend(std::coroutine_handle<promise_type> h) noexcept;
      void await_resume() noexcept {}
    };
    final_awaitable final_suspend() noexcept { return {}; }
    void unhandled_exception() { std::terminate(); }
    void return_void() {}
  };

  using handle_type = std::coroutine_handle<promise_type>;
  handle_type h;

  explicit task(handle_type h_) : h(h_) {}
  task(task &&o) noexcept : h(std::exchange(o.h, {})) {}
  task &operator=(task &&o) noexcept {
    if (this != &o) {
      if (h)
        h.destroy();
      h = std::exchange(o.h, {});
    }
    return *this;
  }
  ~task() {
    if (h)
      h.destroy();
  }

  void detach() { h = nullptr; }

  bool await_ready() const noexcept { return false; }
  void await_suspend(std::coroutine_handle<> caller) const noexcept {
    h.promise().continuation = caller;
    h.resume();
  }
  void await_resume() const noexcept {}
};

inline task task::promise_type::get_return_object() {
  return task{handle_type::from_promise(*this)};
}

// Reactor class definition
struct Reactor {
  Reactor() {
    epfd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (epfd_ < 0)
      throw std::runtime_error("epoll_create1 failed");
  }
  ~Reactor() {
    if (epfd_ >= 0)
      ::close(epfd_);
  }

  struct ReadAwaiter {
    Reactor *r;
    int fd;
    bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<> h) {
      r->add_waiter(fd, EPOLLIN, h);
    }
    void await_resume() const noexcept {}
  };
  ReadAwaiter wait_readable(int fd) { return ReadAwaiter{this, fd}; }

  struct WriteAwaiter {
    Reactor *r;
    int fd;
    bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<> h) {
      r->add_waiter(fd, EPOLLOUT, h);
    }
    void await_resume() const noexcept {}
  };
  WriteAwaiter wait_writable(int fd) { return WriteAwaiter{this, fd}; }

  struct SleepAwaiter {
    Reactor *r;
    int tfd{-1};
    explicit SleepAwaiter(Reactor *r_, std::chrono::nanoseconds ns) : r(r_) {
      tfd = ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
      if (tfd < 0)
        throw std::runtime_error("timerfd_create failed");
      itimerspec its{};
      auto secs = std::chrono::duration_cast<std::chrono::seconds>(ns);
      auto nsec =
          std::chrono::duration_cast<std::chrono::nanoseconds>(ns - secs);
      its.it_value.tv_sec = secs.count();
      its.it_value.tv_nsec = nsec.count();
      if (::timerfd_settime(tfd, 0, &its, nullptr) < 0) {
        ::close(tfd);
        tfd = -1;
        throw std::runtime_error("timerfd_settime failed");
      }
    }
    bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<> h) {
      r->add_waiter(tfd, EPOLLIN, h, true);
    }
    void await_resume() {
      uint64_t expirations = 0;
      (void)::read(tfd, &expirations, sizeof(expirations));
      ::close(tfd);
    }
  };
  SleepAwaiter sleep_for(std::chrono::nanoseconds ns) {
    return SleepAwaiter{this, ns};
  }

  void spawn(task t);

  void run();

private:
  // Friend whole task to allow promise_type access
  friend struct task;

  int epfd_{-1};
  int active_tasks_{0};

  struct Waiters {
    std::vector<std::coroutine_handle<>> rd;
    std::vector<std::coroutine_handle<>> wr;
    uint32_t mask{0};
    bool is_timer{false};
  };
  std::unordered_map<int, Waiters> fds_;

  void ctl_add_or_mod(int fd, uint32_t newmask);
  void add_waiter(int fd, uint32_t edge, std::coroutine_handle<> h,
                  bool is_timer = false);
  std::vector<std::coroutine_handle<>> take_waiters(int fd, uint32_t edge);
};

inline void Reactor::ctl_add_or_mod(int fd, uint32_t newmask) {
  epoll_event ev{};
  ev.data.fd = fd;
  ev.events = newmask;
  if (fds_[fd].mask == 0) {
    if (::epoll_ctl(epfd_, EPOLL_CTL_ADD, fd, &ev) < 0)
      throw std::runtime_error("epoll_ctl ADD failed");
  } else {
    if (::epoll_ctl(epfd_, EPOLL_CTL_MOD, fd, &ev) < 0)
      throw std::runtime_error("epoll_ctl MOD failed");
  }
  fds_[fd].mask = newmask;
}

inline void Reactor::add_waiter(int fd, uint32_t edge,
                                std::coroutine_handle<> h, bool is_timer) {
  auto &w = fds_[fd];
  w.is_timer = w.is_timer || is_timer;
  if (edge & EPOLLIN)
    w.rd.push_back(h);
  if (edge & EPOLLOUT)
    w.wr.push_back(h);
  uint32_t want = w.mask | edge;
  ctl_add_or_mod(fd, want);
}

inline std::vector<std::coroutine_handle<>>
Reactor::take_waiters(int fd, uint32_t edge) {
  std::vector<std::coroutine_handle<>> out;
  auto it = fds_.find(fd);
  if (it == fds_.end())
    return out;
  auto &w = it->second;
  if (edge & EPOLLIN) {
    out.insert(out.end(), w.rd.begin(), w.rd.end());
    w.rd.clear();
  }
  if (edge & EPOLLOUT) {
    out.insert(out.end(), w.wr.begin(), w.wr.end());
    w.wr.clear();
  }
  uint32_t newmask = 0;
  if (!w.rd.empty())
    newmask |= EPOLLIN;
  if (!w.wr.empty())
    newmask |= EPOLLOUT;
  if (newmask != w.mask) {
    if (newmask == 0) {
      (void)::epoll_ctl(epfd_, EPOLL_CTL_DEL, fd, nullptr);
      fds_.erase(it);
    } else {
      ctl_add_or_mod(fd, newmask);
    }
  }
  return out;
}

inline void Reactor::spawn(task t) {
  if (!t.h)
    return;
  t.h.promise().reactor = this;
  ++active_tasks_;
  t.h.resume();
  t.detach();
}

inline void Reactor::run() {
  std::vector<epoll_event> evs(32);
  while (active_tasks_ > 0) {
    int n = ::epoll_wait(epfd_, evs.data(), static_cast<int>(evs.size()), -1);
    if (n < 0) {
      if (errno == EINTR)
        continue;
      throw std::runtime_error("epoll_wait failed");
    }
    for (int i = 0; i < n; ++i) {
      int fd = evs[i].data.fd;
      uint32_t flags = evs[i].events;
      if (flags & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
        auto rw = take_waiters(fd, EPOLLIN | EPOLLOUT);
        for (auto h : rw)
          if (h && !h.done())
            h.resume();
        continue;
      }
      if (flags & EPOLLIN) {
        auto readers = take_waiters(fd, EPOLLIN);
        for (auto h : readers)
          if (h && !h.done())
            h.resume();
      }
      if (flags & EPOLLOUT) {
        auto writers = take_waiters(fd, EPOLLOUT);
        for (auto h : writers)
          if (h && !h.done())
            h.resume();
      }
    }
  }
}

// Coroutine helpers and utilities
inline int set_nonblock(int fd) {
  int flags = fcntl(fd, F_GETFL, 0);
  if (flags == -1)
    return -1;
  return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

inline task async_connect(Reactor &r, int fd, const sockaddr_in &addr) {
  int rc =
      ::connect(fd, reinterpret_cast<const sockaddr *>(&addr), sizeof(addr));
  if (rc == 0)
    co_return;
  if (rc < 0 && errno == EINPROGRESS) {
    co_await r.wait_writable(fd);
    int err{};
    socklen_t len = sizeof(err);
    if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) < 0 || err != 0)
      throw std::runtime_error("connect failed");
    co_return;
  }
  throw std::runtime_error("connect error");
}

inline task async_send_all(Reactor &r, int fd, const uint8_t *data,
                           size_t len) {
  size_t off = 0;
  while (off < len) {
    ssize_t n = ::send(fd, data + off, len - off, 0);
    if (n > 0) {
      off += static_cast<size_t>(n);
    } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
      co_await r.wait_writable(fd);
    } else {
      throw std::runtime_error("send error");
    }
  }
  co_return;
}

inline task async_read_exact(Reactor &r, int fd, uint8_t *buf, size_t total) {
  size_t off = 0;
  while (off < total) {
    ssize_t n = ::recv(fd, buf + off, total - off, 0);
    if (n > 0) {
      off += static_cast<size_t>(n);
    } else if (n == 0) {
      throw std::runtime_error("peer closed");
    } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
      co_await r.wait_readable(fd);
    } else {
      throw std::runtime_error("recv error");
    }
  }
  co_return;
}

inline std::vector<uint8_t> build_frame(const heart_beat::v1::HeartBeat &hb) {
  std::string payload;
  if (!hb.SerializeToString(&payload))
    throw std::runtime_error("Failed to serialize heartbeat");
  uint32_t be = htonl(static_cast<uint32_t>(payload.size()));
  std::vector<uint8_t> frame(4 + payload.size());
  memcpy(frame.data(), &be, 4);
  memcpy(frame.data() + 4, payload.data(), payload.size());
  return frame;
}

inline bool resolve_ipv4(const std::string &host, uint16_t port,
                         sockaddr_in &out) {
  memset(&out, 0, sizeof(out));
  out.sin_family = AF_INET;
  out.sin_port = htons(port);
  if (::inet_pton(AF_INET, host.c_str(), &out.sin_addr) == 1)
    return true;
  hostent *he = ::gethostbyname(host.c_str());
  if (!he || he->h_addrtype != AF_INET)
    return false;
  memcpy(&out.sin_addr, he->h_addr, sizeof(in_addr));
  return true;
}

inline task send_heartbeats(Reactor &r, int sfd, int server_id, const std::string& local_ip_port) {
  heart_beat::v1::HeartBeat hb;
  hb.set_server_id(server_id);
  hb.set_ip(local_ip_port);
  while (true) {
    *hb.mutable_timestamp() =
        google::protobuf::util::TimeUtil::GetCurrentTime();

    // Populate system metrics from cached monitor if available
    auto& monitor = CachedSystemMonitor::instance();
    if (monitor.has_sample()) {
      auto usage = monitor.get();
      hb.set_cpu_usage(usage.cpu_usage);
      hb.set_total_storage_used(usage.disk_usage);
      hb.set_ram_usage(usage.ram_usage);
      hb.set_disk_usage(usage.disk_usage);
      hb.set_network_in(usage.network_in);
      hb.set_network_out(usage.network_out);
      hb.set_network_in_bytes_per_sec(usage.network_in_bytes_per_sec);
      hb.set_network_out_bytes_per_sec(usage.network_out_bytes_per_sec);
    } else {
      // Fallback: at least send disk and RAM (non-blocking reads)
      auto [diskGB, diskPct] = getDiskUsageGBPercent();
      auto [ramGB, ramPct] = getRamUsageGBPercent();
      hb.set_total_storage_used(diskPct);
      hb.set_disk_usage(diskPct);
      hb.set_ram_usage(ramPct);
    }

    auto frame = build_frame(hb);
    co_await async_send_all(r, sfd, frame.data(), frame.size());
    co_await r.sleep_for(std::chrono::seconds(1));
  }
}

inline task
recv_heartbeats(Reactor &r, int sfd,
                std::function<void(const heart_beat::v1::HeartBeat &)> on_msg) {
  std::vector<uint8_t> buf;
  uint8_t sz[4];
  while (true) {
    co_await async_read_exact(r, sfd, sz, 4);
    uint32_t be_len;
    memcpy(&be_len, sz, 4);
    uint32_t body_len = ntohl(be_len);
    buf.resize(body_len);
    if (body_len > 0)
      co_await async_read_exact(r, sfd, buf.data(), buf.size());
    heart_beat::v1::HeartBeat hb;
    if (!hb.ParseFromArray(buf.data(), static_cast<int>(buf.size())))
      throw std::runtime_error("ParseFromArray failed");
    on_msg(hb);
  }
}

inline int send_signal(std::string server_ip, int server_id, int port = 9000, std::string local_ip_port = "") {
  try {
    sockaddr_in addr{};
    if (!resolve_ipv4(server_ip, static_cast<uint16_t>(port), addr)) {
      std::cerr << "Could not resolve hostname\n";
      return 1;
    }
    int sfd = ::socket(AF_INET, SOCK_DGRAM | SOCK_CLOEXEC, 0);
    if (sfd < 0) {
      std::cerr << "Socket creation failed\n";
      return 1;
    }
    if (set_nonblock(sfd) < 0) {
      std::cerr << "Failed to set nonblocking\n";
      ::close(sfd);
      return 1;
    }

    Reactor r;
    r.spawn(async_connect(r, sfd, addr));
    r.spawn(send_heartbeats(r, sfd, server_id, local_ip_port));
    r.run();
    ::close(sfd);
    return 0;
  } catch (const std::exception &e) {
    std::cerr << "send_signal error: " << e.what() << "\n";
    return 1;
  }
}

inline int recieve_signal(int port = 9000) {
  try {
    int lfd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (lfd < 0) {
      std::cerr << "Listen socket failed\n";
      return 1;
    }
    int yes = 1;
    setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if (::bind(lfd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
      std::cerr << "Bind failed\n";
      ::close(lfd);
      return 1;
    }
    if (::listen(lfd, 8) < 0) {
      std::cerr << "Listen failed\n";
      ::close(lfd);
      return 1;
    }
    if (set_nonblock(lfd) < 0) {
      std::cerr << "Failed to set nonblocking\n";
      ::close(lfd);
      return 1;
    }

    Reactor r;

    auto accept_and_recv = [&]() -> task {
      int cfd = -1;
      while (true) {
        sockaddr_in peer{};
        socklen_t len = sizeof(peer);
        cfd = ::accept4(lfd, reinterpret_cast<sockaddr *>(&peer), &len,
                        SOCK_CLOEXEC | SOCK_NONBLOCK);
        if (cfd >= 0)
          break;
        if (errno == EAGAIN || errno == EWOULDBLOCK)
          co_await r.wait_readable(lfd);
        else
          throw std::runtime_error("accept error");
      }
      ::close(lfd);
      r.spawn(
          recv_heartbeats(r, cfd, [cfd](const heart_beat::v1::HeartBeat &hb) {
            std::cout << "Heartbeat: server_id=" << hb.server_id()
                      << " timestamp=" << hb.timestamp().seconds() << "."
                      << hb.timestamp().nanos() << "\n";
            (void)cfd;
          }));
      co_return;
    };

    r.spawn(accept_and_recv());
    r.run();
    return 0;
  } catch (const std::exception &e) {
    std::cerr << "recieve_signal error: " << e.what() << "\n";
    return 1;
  }
}

// Implementation of final_awaitable::await_suspend after Reactor is fully defined
inline void task::promise_type::final_awaitable::await_suspend(
    std::coroutine_handle<promise_type> h) noexcept {
  auto &p = h.promise();
  if (p.reactor) {
    --p.reactor->active_tasks_;
  }
  if (p.continuation) {
    p.continuation.resume();
  } else {
    h.destroy();
  }
}

} // namespace async_hb
