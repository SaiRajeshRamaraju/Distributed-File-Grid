#include "optimized_heartbeat_server.hpp"
#include "metrics_exporter.hpp"
#include <system_error>
#include <sstream>
#include <iomanip>
#include <csignal>

namespace {
    volatile std::sig_atomic_t g_signal_status = 0;
    
    void signal_handler(int signal) {
        g_signal_status = signal;
    }
    
    void set_nonblocking(int fd) {
        int flags = fcntl(fd, F_GETFL, 0);
        if (flags == -1) {
            throw std::system_error(errno, std::system_category(), "fcntl F_GETFL failed");
        }
        if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
            throw std::system_error(errno, std::system_category(), "fcntl F_SETFL failed");
        }
    }
    
    void set_socket_options(int fd) {
        int enable = 1;
        if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(enable)) < 0) {
            throw std::system_error(errno, std::system_category(), "setsockopt(SO_REUSEADDR) failed");
        }
        
        int keepalive = 1;
        if (setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &keepalive, sizeof(keepalive)) < 0) {
            throw std::system_error(errno, std::system_category(), "setsockopt(SO_KEEPALIVE) failed");
        }
        
        int keepidle = 30;  // Start keepalive after 30s of inactivity
        int keepintvl = 10; // Send keepalive every 10s
        int keepcnt = 3;    // Number of keepalive probes before dropping connection
        
        if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, &keepidle, sizeof(keepidle)) < 0) {
            throw std::system_error(errno, std::system_category(), "setsockopt(TCP_KEEPIDLE) failed");
        }
        if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, &keepintvl, sizeof(keepintvl)) < 0) {
            throw std::system_error(errno, std::system_category(), "setsockopt(TCP_KEEPINTVL) failed");
        }
        if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT, &keepcnt, sizeof(keepcnt)) < 0) {
            throw std::system_error(errno, std::system_category(), "setsockopt(TCP_KEEPCNT) failed");
        }
    }
    
    std::string get_client_address(const sockaddr_in& client_addr) {
        char ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &(client_addr.sin_addr), ip, INET_ADDRSTRLEN);
        std::stringstream ss;
        ss << ip << ":" << ntohs(client_addr.sin_port);
        return ss.str();
    }
}

OptimizedHeartbeatServer::OptimizedHeartbeatServer(int port, size_t worker_threads)
    : port_(port),
      worker_threads_count_(worker_threads > 0 ? worker_threads : std::thread::hardware_concurrency()),
      running_(false),
      metrics_exporter_(std::make_unique<MetricsExporter>("0.0.0.0:9091")) {
    
    epoll_fd_ = epoll_create1(0);
    if (epoll_fd_ == -1) {
        throw std::system_error(errno, std::system_category(), "epoll_create1 failed");
    }
    
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);
}
