#pragma once

#include <atomic>
#include <thread>
#include <vector>
#include <unordered_map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <queue>
#include <condition_variable>
#include <functional>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <cstring>
#include <system_error>
#include <stdexcept>
#include <iostream>
#include <sstream>
#include <iomanip>
#include <csignal>
#include <chrono>

class OptimizedHeartbeatServer {
public:
    using ClientId = int;
    using Timestamp = std::chrono::system_clock::time_point;
    
    struct ClientInfo {
        int fd;
        Timestamp last_heartbeat;
        std::string address;
        std::vector<uint8_t> buffer;
        static constexpr size_t BUFFER_SIZE = 4096;
        
        ClientInfo(int fd, const std::string& addr) 
            : fd(fd), last_heartbeat(std::chrono::system_clock::now()), address(addr) {
            buffer.reserve(BUFFER_SIZE);
        }
        
        ~ClientInfo() {
            if (fd != -1) {
                ::shutdown(fd, SHUT_RDWR);
                ::close(fd);
            }
        }
        
        // Disable copying
        ClientInfo(const ClientInfo&) = delete;
        ClientInfo& operator=(const ClientInfo&) = delete;
        
        // Enable moving
        ClientInfo(ClientInfo&&) = default;
        ClientInfo& operator=(ClientInfo&&) = default;
    };
    
    struct Metrics {
        std::atomic<uint64_t> total_received_messages{0};
        std::atomic<uint64_t> total_clients_connected{0};
        std::atomic<uint64_t> total_bytes_received{0};
        std::atomic<uint64_t> total_processing_time_ns{0};
        
        // Make Metrics copyable
        Metrics() = default;
        Metrics(const Metrics& other) {
            total_received_messages = other.total_received_messages.load();
            total_clients_connected = other.total_clients_connected.load();
            total_bytes_received = other.total_bytes_received.load();
            total_processing_time_ns = other.total_processing_time_ns.load();
        }
        
        Metrics& operator=(const Metrics& other) {
            if (this != &other) {
                total_received_messages = other.total_received_messages.load();
                total_clients_connected = other.total_clients_connected.load();
                total_bytes_received = other.total_bytes_received.load();
                total_processing_time_ns = other.total_processing_time_ns.load();
            }
            return *this;
        }
        
        void reset() {
            total_received_messages = 0;
            total_clients_connected = 0;
            total_bytes_received = 0;
            total_processing_time_ns = 0;
        }
    };
    
    OptimizedHeartbeatServer(int port, 
                          size_t worker_threads = std::thread::hardware_concurrency(),
                          const std::string& metrics_bind_address = "0.0.0.0:9091")
        : port_(port), 
          worker_threads_count_(worker_threads > 0 ? worker_threads : 1),
          running_(false),
          metrics_exporter_(std::make_unique<MetricsExporter>(metrics_bind_address)) {
        // Initialize epoll
        epoll_fd_ = epoll_create1(0);
        if (epoll_fd_ == -1) {
            throw std::runtime_error("Failed to create epoll instance: " + std::string(strerror(errno)));
        }
        
        // Setup signal handling
        setup_signal_handling();
    }
    
    ~OptimizedHeartbeatServer() {
        stop();
        if (epoll_fd_ != -1) {
            ::close(epoll_fd_);
            epoll_fd_ = -1;
        }
    }
    
    void start() {
        if (running_.exchange(true)) {
            return; // Already running
        }
        
        // Start the acceptor thread
        acceptor_thread_ = std::thread(&OptimizedHeartbeatServer::acceptor_loop, this);
        
        // Start worker threads
        for (size_t i = 0; i < worker_threads_count_; ++i) {
            worker_threads_.emplace_back(&OptimizedHeartbeatServer::worker_loop, this);
        }
        
        // Start the cleanup thread
        cleanup_thread_ = std::thread(&OptimizedHeartbeatServer::cleanup_loop, this);
    }
    
    void stop() {
        if (!running_.exchange(false)) {
            return; // Already stopped
        }
        
        // Signal all threads to stop
        if (acceptor_thread_.joinable()) {
            acceptor_thread_.join();
        }
        
        // Signal worker threads
        {
            std::unique_lock<std::mutex> lock(task_mutex_);
            cv_.notify_all();
        }
        
        // Join worker threads
        for (size_t i = 0; i < worker_threads_.size(); ++i) {
            if (worker_threads_[i].joinable()) {
                worker_threads_[i].join();
            }
        }
        
        // Join cleanup thread
        if (cleanup_thread_.joinable()) {
            cleanup_thread_.join();
        }
        
        // Close all client connections
        close_all_connections();
    }
    
    Metrics get_metrics() const {
        return metrics_;
    }
    
    void reset_metrics() {
        metrics_.reset();
    }
    
private:
    void setup_signal_handling() {
        // Ignore SIGPIPE to handle broken pipes gracefully
        struct sigaction sa;
        sa.sa_handler = SIG_IGN;
        sigemptyset(&sa.sa_mask);
        sa.sa_flags = 0;
        if (sigaction(SIGPIPE, &sa, nullptr) == -1) {
            throw std::runtime_error("Failed to set up SIGPIPE handler: " + std::string(strerror(errno)));
        }
    }
    
    void acceptor_loop() {
        // Create listening socket
        int listen_fd = create_listening_socket();
        
        // Add listening socket to epoll
        struct epoll_event ev;
        ev.events = EPOLLIN | EPOLLET; // Edge-triggered mode
        ev.data.fd = listen_fd;
        
        if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, listen_fd, &ev) == -1) {
            throw std::runtime_error("Failed to add listening socket to epoll: " + std::string(strerror(errno)));
        }
        
        // Event buffer for epoll_wait
        const int MAX_EVENTS = 64;
        struct epoll_event events[MAX_EVENTS];
        
        while (running_) {
            int nfds = epoll_wait(epoll_fd_, events, MAX_EVENTS, 100); // 100ms timeout
            
            if (nfds == -1) {
                if (errno == EINTR) {
                    continue; // Interrupted by signal
                }
                throw std::runtime_error("epoll_wait failed: " + std::string(strerror(errno)));
            }
            
            for (int i = 0; i < nfds; ++i) {
                if (events[i].data.fd == listen_fd) {
                    // New connection
                    handle_new_connection(listen_fd);
                } else {
                    // Existing client has data to read
                    int client_fd = events[i].data.fd;
                    {
                        std::shared_lock<std::shared_mutex> lock(clients_mutex_);
                        if (clients_.find(client_fd) != clients_.end()) {
                            // Add task to worker queue
                            std::lock_guard<std::mutex> task_lock(task_mutex_);
                            task_queue_.push(client_fd);
                            cv_.notify_one();
                        }
                    }
                }
            }
        }
        
        // Cleanup
        ::close(listen_fd);
    }
    
    void worker_loop() {
        while (running_) {
            int client_fd = -1;
            
            // Get next task
            {
                std::unique_lock<std::mutex> lock(task_mutex_);
                cv_.wait(lock, [this] { 
                    return !running_ || !task_queue_.empty(); 
                });
                
                if (!running_) {
                    break;
                }
                
                if (!task_queue_.empty()) {
                    client_fd = task_queue_.front();
                    task_queue_.pop();
                }
            }
            
            if (client_fd != -1) {
                process_client_data(client_fd);
            }
        }
    }
    
    void cleanup_loop() {
        constexpr auto CLEANUP_INTERVAL = std::chrono::seconds(30);
        constexpr auto CLIENT_TIMEOUT = std::chrono::seconds(60);
        
        while (running_) {
            auto now = std::chrono::system_clock::now();
            std::vector<int> clients_to_remove;
            
            // Find timed out clients
            {
                std::shared_lock<std::shared_mutex> lock(clients_mutex_);
                for (const auto& [fd, client] : clients_) {
                    if (now - client->last_heartbeat > CLIENT_TIMEOUT) {
                        clients_to_remove.push_back(fd);
                    }
                }
            }
            
            // Remove timed out clients
            for (int fd : clients_to_remove) {
                remove_client(fd);
            }
            
            // Sleep until next cleanup
            std::this_thread::sleep_for(CLEANUP_INTERVAL);
        }
    }
    
    int create_listening_socket() {
        int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
        if (fd == -1) {
            throw std::runtime_error("Failed to create socket: " + std::string(strerror(errno)));
        }
        
        // Set SO_REUSEADDR to allow quick reuse of the port
        int opt = 1;
        if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) == -1) {
            ::close(fd);
            throw std::runtime_error("Failed to set SO_REUSEADDR: " + std::string(strerror(errno)));
        }
        
        // Bind to port
        struct sockaddr_in addr;
        std::memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons(port_);
        
        if (bind(fd, (struct sockaddr*)&addr, sizeof(addr)) == -1) {
            ::close(fd);
            throw std::runtime_error("Failed to bind to port " + std::to_string(port_) + ": " + 
                                   std::string(strerror(errno)));
        }
        
        // Start listening
        if (listen(fd, SOMAXCONN) == -1) {
            ::close(fd);
            throw std::runtime_error("Failed to listen on socket: " + std::string(strerror(errno)));
        }
        
        return fd;
    }
    
    void handle_new_connection(int listen_fd) {
        struct sockaddr_in client_addr;
        socklen_t client_addr_len = sizeof(client_addr);
        
        while (true) {
            int client_fd = accept4(listen_fd, (struct sockaddr*)&client_addr, 
                                  &client_addr_len, SOCK_NONBLOCK);
            
            if (client_fd == -1) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    // No more connections to accept
                    break;
                } else {
                    // Log error and continue
                    std::cerr << "Accept failed: " << strerror(errno) << std::endl;
                    continue;
                }
            }
            
            // Get client address
            char client_ip[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, INET_ADDRSTRLEN);
            std::string client_address = std::string(client_ip) + ":" + 
                                       std::to_string(ntohs(client_addr.sin_port));
            
            // Add to epoll
            struct epoll_event ev;
            ev.events = EPOLLIN | EPOLLET | EPOLLRDHUP | EPOLLONESHOT;
            ev.data.fd = client_fd;
            
            if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, client_fd, &ev) == -1) {
                std::cerr << "Failed to add client to epoll: " << strerror(errno) << std::endl;
                ::close(client_fd);
                continue;
            }
            
            // Create and store client info
            auto client_info = std::make_shared<ClientInfo>(client_fd, client_address);
            
            {
                std::unique_lock<std::shared_mutex> lock(clients_mutex_);
                clients_[client_fd] = client_info;
            }
            
            metrics_.total_clients_connected++;
            
            std::cout << "New connection from " << client_address << " (FD: " << client_fd << ")" << std::endl;
        }
    }
    
    void process_client_data(int client_fd) {
        auto start_time = std::chrono::high_resolution_clock::now();
        
        std::shared_ptr<ClientInfo> client;
        {
            std::shared_lock<std::shared_mutex> lock(clients_mutex_);
            auto it = clients_.find(client_fd);
            if (it == clients_.end()) {
                return; // Client already removed
            }
            client = it->second;
        }
        
        // Read data from client
        ssize_t bytes_read = 0;
        char buffer[4096];
        
        while (true) {
            bytes_read = recv(client_fd, buffer, sizeof(buffer), 0);
            
            if (bytes_read == -1) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    // No more data to read
                    break;
                } else {
                    // Error reading from client
                    remove_client(client_fd);
                    return;
                }
            } else if (bytes_read == 0) {
                // Client disconnected
                remove_client(client_fd);
                return;
            }
            
            // Update metrics
            metrics_.total_bytes_received += bytes_read;
            
            // Process the received data
            process_heartbeat_data(client, buffer, bytes_read);
        }
        
        // Update last heartbeat time
        client->last_heartbeat = std::chrono::system_clock::now();
        
        // Re-enable EPOLLIN event for this client
        struct epoll_event ev;
        ev.events = EPOLLIN | EPOLLET | EPOLLRDHUP | EPOLLONESHOT;
        ev.data.fd = client_fd;
        
        if (epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, client_fd, &ev) == -1) {
            std::cerr << "Failed to re-enable EPOLLIN for client " << client_fd << ": " 
                     << strerror(errno) << std::endl;
            remove_client(client_fd);
            return;
        }
        
        // Update metrics
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
            end_time - start_time);
        metrics_.total_processing_time_ns += duration.count();
    }
    
    void process_heartbeat_data(const std::shared_ptr<ClientInfo>& client, 
                               const char* data, size_t size) {
        // In a real implementation, this would parse the heartbeat message
        // and update the server state accordingly.
        
        // For now, we'll just count the message and log it
        metrics_.total_received_messages++;
        
        if (metrics_.total_received_messages % 1000 == 0) {
            std::cout << "Processed " << metrics_.total_received_messages 
                     << " heartbeat messages" << std::endl;
        }
    }
    
    void remove_client(int client_fd) {
        std::unique_lock<std::shared_mutex> lock(clients_mutex_);
        auto it = clients_.find(client_fd);
        if (it != clients_.end()) {
            std::cout << "Client disconnected: " << it->second->address 
                     << " (FD: " << client_fd << ")" << std::endl;
            
            // Remove from epoll
            epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, client_fd, nullptr);
            
            // Erase from clients map (ClientInfo destructor will close the socket)
            clients_.erase(it);
        }
    }
    
    void close_all_connections() {
        std::unique_lock<std::shared_mutex> lock(clients_mutex_);
        for (auto& [fd, client] : clients_) {
            // ClientInfo destructor will close the socket
            epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
        }
        clients_.clear();
    }
    
private:
    const int port_;
    const size_t worker_threads_count_;
    std::atomic<bool> running_;
    std::unique_ptr<MetricsExporter> metrics_exporter_;
    
    int epoll_fd_ = -1;
    std::thread acceptor_thread_;
    std::vector<std::thread> worker_threads_;
    std::thread cleanup_thread_;
    
    std::unordered_map<int, std::shared_ptr<ClientInfo>> clients_;
    mutable std::shared_mutex clients_mutex_;
    
    // Task queue for worker threads
    std::queue<int> task_queue_;
    std::mutex task_mutex_;
    std::condition_variable cv_;
    
    // Metrics
    Metrics metrics_;
};
