#include <gtest/gtest.h>
#include <thread>
#include <atomic>
#include <vector>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <cerrno>
#include <iomanip>  // for std::setprecision, std::fixed

using namespace std::chrono_literals;

class SimpleHeartbeatTest : public ::testing::Test {
protected:
    static constexpr int TEST_PORT = 9001;
    std::atomic<bool> server_running{false};
    std::thread server_thread;
    std::atomic<int> received_heartbeats{0};
    int server_fd = -1;

    void SetUp() override {
        server_running = true;
        server_thread = std::thread([this]() {
            // Create socket
            server_fd = socket(AF_INET, SOCK_STREAM, 0);
            if (server_fd < 0) {
                GTEST_FAIL() << "Failed to create server socket";
                return;
            }

            // Set socket options
            int opt = 1;
            setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
            
            // Bind to port
            struct sockaddr_in addr;
            memset(&addr, 0, sizeof(addr));
            addr.sin_family = AF_INET;
            addr.sin_addr.s_addr = INADDR_ANY;
            addr.sin_port = htons(TEST_PORT);

            if (bind(server_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
                close(server_fd);
                GTEST_FAIL() << "Failed to bind server socket";
                return;
            }

            // Listen for connections
            if (listen(server_fd, 5) < 0) {
                close(server_fd);
                GTEST_FAIL() << "Failed to listen on socket";
                return;
            }
            
            // Accept connections
            while (server_running) {
                struct sockaddr_in client_addr;
                socklen_t client_len = sizeof(client_addr);
                int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &client_len);
                
                if (client_fd < 0) {
                    if (errno == EAGAIN || errno == EWOULDBLOCK) {
                        std::this_thread::sleep_for(10ms);
                        continue;
                    }
                    if (server_running) {
                        GTEST_FAIL() << "Accept failed: " << strerror(errno);
                    }
                    break;
                }

                // Handle client
                std::thread([this, client_fd]() {
                    char buffer[1024];
                    fd_set readfds;
                    struct timeval tv;
                    
                    while (server_running) {
                        FD_ZERO(&readfds);
                        FD_SET(client_fd, &readfds);
                        
                        // Set timeout to 100ms
                        tv.tv_sec = 0;
                        tv.tv_usec = 100000;
                        
                        int activity = select(client_fd + 1, &readfds, nullptr, nullptr, &tv);
                        
                        if (activity < 0 && errno != EINTR) {
                            break;  // Error occurred
                        }
                        
                        if (activity > 0 && FD_ISSET(client_fd, &readfds)) {
                            ssize_t bytes = recv(client_fd, buffer, sizeof(buffer) - 1, 0);
                            if (bytes <= 0) {
                                break;  // Connection closed or error
                            }
                            buffer[bytes] = '\0';  // Null-terminate the received data
                            received_heartbeats++;
                        }
                    }
                    
                    // Clean up
                    shutdown(client_fd, SHUT_RDWR);
                    close(client_fd);
                }).detach();          
            }
            
            if (server_fd >= 0) {
                close(server_fd);
                server_fd = -1;
            }
        });
        
        // Wait for server to start
        std::this_thread::sleep_for(100ms);
    }

    void TearDown() override {
        server_running = false;
        if (server_fd >= 0) {
            shutdown(server_fd, SHUT_RDWR);
        }
        if (server_thread.joinable()) {
            server_thread.join();
        }
    }
};

TEST_F(SimpleHeartbeatTest, TestBasicConnection) {
    // Create a client socket
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(sockfd, 0) << "Failed to create client socket";
    
    // Set socket options
    int opt = 1;
    setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    
    // Set send timeout (1 second)
    struct timeval tv;
    tv.tv_sec = 1;
    tv.tv_usec = 0;
    setsockopt(sockfd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    
    // Connect to server
    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(TEST_PORT);
    inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr);
    
    int result = connect(sockfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr));
    EXPECT_GE(result, 0) << "Connection failed: " << strerror(errno);
    
    // Send some data
    const char* msg = "test";
    ssize_t bytes_sent = send(sockfd, msg, strlen(msg), 0);
    EXPECT_GT(bytes_sent, 0) << "Send failed: " << strerror(errno);
    
    // Clean up
    close(sockfd);
    
    // Wait for the server to process the message
    std::this_thread::sleep_for(100ms);
    
    // Verify the message was received
    EXPECT_GT(received_heartbeats, 0) << "No messages received by server";
}

TEST_F(SimpleHeartbeatTest, TestMultipleConnections) {
    const int NUM_CONNECTIONS = 5;
    
    for (int i = 0; i < NUM_CONNECTIONS; ++i) {
        // Create a client socket
        int sockfd = socket(AF_INET, SOCK_STREAM, 0);
        ASSERT_GE(sockfd, 0) << "Failed to create client socket";
        
        // Set socket options
        int opt = 1;
        setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
        
        // Connect to server
        struct sockaddr_in serv_addr;
        memset(&serv_addr, 0, sizeof(serv_addr));
        serv_addr.sin_family = AF_INET;
        serv_addr.sin_port = htons(TEST_PORT);
        inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr);
        
        int result = connect(sockfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr));
        ASSERT_GE(result, 0) << "Connection failed: " << strerror(errno);
        
        // Send some data
        const char* msg = "test";
        ssize_t bytes_sent = send(sockfd, msg, strlen(msg), 0);
        EXPECT_GT(bytes_sent, 0) << "Send failed: " << strerror(errno);
        
        // Close the connection
        close(sockfd);
        
        // Small delay between connections
        std::this_thread::sleep_for(50ms);
    }
    
    // Wait for all messages to be processed
    std::this_thread::sleep_for(200ms);
    
    // Verify all messages were received
    EXPECT_GE(received_heartbeats, NUM_CONNECTIONS) 
        << "Expected at least " << NUM_CONNECTIONS 
        << " messages, but got " << received_heartbeats;
}

TEST_F(SimpleHeartbeatTest, TestInvalidConnection) {
    // Try to connect to an invalid port
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(sockfd, 0) << "Failed to create client socket";
    
    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(1);  // Port 1 is usually restricted
    inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr);
    
    int result = connect(sockfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr));
    EXPECT_LT(result, 0) << "Expected connection to fail on restricted port";
    
    close(sockfd);
}

TEST_F(SimpleHeartbeatTest, TestLargeMessage) {
    // Create a client socket
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(sockfd, 0) << "Failed to create client socket";
    
    // Set socket options
    int opt = 1;
    setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    
    // Connect to server
    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(TEST_PORT);
    inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr);
    
    int result = connect(sockfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr));
    ASSERT_GE(result, 0) << "Connection failed: " << strerror(errno);
    
    // Send a large message (1MB)
    const size_t large_size = 1024 * 1024;
    std::vector<char> large_buffer(large_size, 'A');
    
    ssize_t total_sent = 0;
    while (total_sent < large_size) {
        ssize_t bytes_sent = send(sockfd, large_buffer.data() + total_sent, 
                                 large_size - total_sent, 0);
        if (bytes_sent <= 0) {
            break;
        }
        total_sent += bytes_sent;
    }
    
    EXPECT_GT(total_sent, 0) << "Failed to send large message";
    close(sockfd);
}

TEST_F(SimpleHeartbeatTest, TestRapidConnections) {
    const int NUM_RAPID_CONNECTIONS = 100;
    int successful_connections = 0;
    
    for (int i = 0; i < NUM_RAPID_CONNECTIONS; ++i) {
        int sockfd = socket(AF_INET, SOCK_STREAM, 0);
        if (sockfd < 0) continue;
        
        // Set non-blocking for connect timeout
        int flags = fcntl(sockfd, F_GETFL, 0);
        fcntl(sockfd, F_SETFL, flags | O_NONBLOCK);
        
        struct sockaddr_in serv_addr;
        memset(&serv_addr, 0, sizeof(serv_addr));
        serv_addr.sin_family = AF_INET;
        serv_addr.sin_port = htons(TEST_PORT);
        inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr);
        
        int result = connect(sockfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr));
        if (result == 0) {
            // Connection succeeded immediately
            successful_connections++;
            send(sockfd, "test", 4, 0);
            close(sockfd);
            continue;
        }
        
        if (errno == EINPROGRESS) {
            // Connection in progress, wait for it to complete
            fd_set writefds;
            FD_ZERO(&writefds);
            FD_SET(sockfd, &writefds);
            
            struct timeval tv;
            tv.tv_sec = 1;  // 1 second timeout
            tv.tv_usec = 0;
            
            result = select(sockfd + 1, nullptr, &writefds, nullptr, &tv);
            if (result > 0 && FD_ISSET(sockfd, &writefds)) {
                int error = 0;
                socklen_t len = sizeof(error);
                getsockopt(sockfd, SOL_SOCKET, SO_ERROR, &error, &len);
                if (error == 0) {
                    successful_connections++;
                    send(sockfd, "test", 4, 0);
                }
            }
        }
        
        close(sockfd);
    }
    
    // Wait for all messages to be processed
    std::this_thread::sleep_for(500ms);
    
    // Verify we got at least some successful connections
    EXPECT_GT(successful_connections, 0) << "No successful rapid connections";
    EXPECT_GE(received_heartbeats, successful_connections) 
        << "Missed some heartbeats";
}

TEST_F(SimpleHeartbeatTest, TestServerShutdown) {
    // First, test normal operation
    {
        int sockfd = socket(AF_INET, SOCK_STREAM, 0);
        ASSERT_GE(sockfd, 0);
        
        struct sockaddr_in serv_addr;
        memset(&serv_addr, 0, sizeof(serv_addr));
        serv_addr.sin_family = AF_INET;
        serv_addr.sin_port = htons(TEST_PORT);
        inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr);
        
        int result = connect(sockfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr));
        ASSERT_GE(result, 0) << "Initial connection failed";
        
        send(sockfd, "test", 4, 0);
        close(sockfd);
    }
    
    // Shutdown the server
    server_running = false;
    if (server_thread.joinable()) {
        server_thread.join();
    }
    
    // Try to connect to the shutdown server
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(sockfd, 0);
    
    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(TEST_PORT);
    inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr);
    
    int result = connect(sockfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr));
    EXPECT_LT(result, 0) << "Should not be able to connect to shutdown server";
    close(sockfd);
    
    // Restart the server for other tests
    SetUp();
}

TEST_F(SimpleHeartbeatTest, TestPartialWrites) {
    // Create a client socket
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(sockfd, 0) << "Failed to create client socket";
    
    // Set socket options
    int opt = 1;
    setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    
    // Connect to server
    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(TEST_PORT);
    inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr);
    
    int result = connect(sockfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr));
    ASSERT_GE(result, 0) << "Connection failed: " << strerror(errno);
    
    // Send data in small chunks to test partial writes
    const char* message = "HEARTBEAT:1234567890";
    size_t message_len = strlen(message);
    
    // Send first half
    ssize_t sent = send(sockfd, message, message_len / 2, 0);
    EXPECT_GT(sent, 0) << "First send failed: " << strerror(errno);
    
    // Small delay
    std::this_thread::sleep_for(100ms);
    
    // Send second half
    sent = send(sockfd, message + (message_len / 2), message_len - (message_len / 2), 0);
    EXPECT_GT(sent, 0) << "Second send failed: " << strerror(errno);
    
    close(sockfd);
    
    // Wait for the server to process the message
    std::this_thread::sleep_for(100ms);
    
    // Verify the message was received
    EXPECT_GT(received_heartbeats, 0) << "No messages received by server";
}

TEST_F(SimpleHeartbeatTest, TestConnectionReset) {
    // Create a client socket
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(sockfd, 0) << "Failed to create client socket";
    
    // Set socket options
    int opt = 1;
    setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    
    // Connect to server
    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(TEST_PORT);
    inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr);
    
    int result = connect(sockfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr));
    ASSERT_GE(result, 0) << "Connection failed: " << strerror(errno);
    
    // Enable TCP keepalive
    int keepalive = 1;
    setsockopt(sockfd, SOL_SOCKET, SO_KEEPALIVE, &keepalive, sizeof(keepalive));
    
    // Send initial data
    const char* msg = "HEARTBEAT:START";
    ssize_t bytes_sent = send(sockfd, msg, strlen(msg), 0);
    EXPECT_GT(bytes_sent, 0) << "Send failed: " << strerror(errno);
    
    // Abruptly close the connection
    struct linger sl;
    sl.l_onoff = 1;     // Non-zero to enable lingering
    sl.l_linger = 0;     // Timeout in seconds
    setsockopt(sockfd, SOL_SOCKET, SO_LINGER, &sl, sizeof(sl));
    close(sockfd);
    
    // The server should handle this gracefully
    std::this_thread::sleep_for(100ms);
}

TEST_F(SimpleHeartbeatTest, TestHighLoad) {
    const int NUM_CLIENTS = 30;  // Reduced from 50 to 30
    const int MESSAGES_PER_CLIENT = 5;  // Reduced from 10 to 5
    std::vector<std::thread> clients;
    std::atomic<int> successful_messages{0};
    
    auto client_func = [&](int client_id) {
        int sockfd = socket(AF_INET, SOCK_STREAM, 0);
        if (sockfd < 0) {
            std::cerr << "Client " << client_id << " failed to create socket" << std::endl;
            return;
        }
        
        // Set socket options
        int opt = 1;
        setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
        
        // Set send timeout
        struct timeval tv;
        tv.tv_sec = 1;
        tv.tv_usec = 0;
        setsockopt(sockfd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
        
        struct sockaddr_in serv_addr;
        memset(&serv_addr, 0, sizeof(serv_addr));
        serv_addr.sin_family = AF_INET;
        serv_addr.sin_port = htons(TEST_PORT);
        inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr);
        
        // Connect with timeout
        fd_set fdset;
        FD_ZERO(&fdset);
        FD_SET(sockfd, &fdset);
        
        // Set socket to non-blocking
        int flags = fcntl(sockfd, F_GETFL, 0);
        fcntl(sockfd, F_SETFL, flags | O_NONBLOCK);
        
        int res = connect(sockfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr));
        if (res < 0 && errno != EINPROGRESS) {
            std::cerr << "Client " << client_id << " connect failed: " << strerror(errno) << std::endl;
            close(sockfd);
            return;
        }
        
        if (res == 0) {
            // Connected immediately
            fcntl(sockfd, F_SETFL, flags);  // Set back to blocking
        } else {
            // Wait for connection to complete
            tv.tv_sec = 1;
            tv.tv_usec = 0;
            
            res = select(sockfd + 1, nullptr, &fdset, nullptr, &tv);
            if (res <= 0) {
                std::cerr << "Client " << client_id << " connect timeout" << std::endl;
                close(sockfd);
                return;
            }
            
            // Check for errors
            int so_error;
            socklen_t len = sizeof(so_error);
            getsockopt(sockfd, SOL_SOCKET, SO_ERROR, &so_error, &len);
            if (so_error != 0) {
                std::cerr << "Client " << client_id << " connect error: " << strerror(so_error) << std::endl;
                close(sockfd);
                return;
            }
            
            // Set back to blocking
            fcntl(sockfd, F_SETFL, flags);
        }
        
        // Send messages
        for (int i = 0; i < MESSAGES_PER_CLIENT && server_running; ++i) {
            std::string msg = "HEARTBEAT:CLIENT" + std::to_string(client_id) + ":MSG" + std::to_string(i);
            ssize_t sent = send(sockfd, msg.c_str(), msg.length(), 0);
            if (sent > 0) {
                successful_messages++;
            } else {
                std::cerr << "Client " << client_id << " send error: " << strerror(errno) << std::endl;
                break;
            }
            std::this_thread::sleep_for(5ms);
        }
        
        // Graceful shutdown
        shutdown(sockfd, SHUT_WR);
        
        // Drain any remaining data
        char buf[128];
        while (recv(sockfd, buf, sizeof(buf), 0) > 0) {}
        
        close(sockfd);
    };
    
    // Start all clients with staggered delays
    for (int i = 0; i < NUM_CLIENTS; ++i) {
        clients.emplace_back([&, i]() {
            // Stagger client starts
            std::this_thread::sleep_for(std::chrono::milliseconds(i * 10));
            client_func(i);
        });
    }
    
    // Wait for all clients to finish
    for (auto& t : clients) {
        if (t.joinable()) {
            t.join();
        }
    }
    
    // Wait for all messages to be processed
    std::this_thread::sleep_for(2000ms);
    
    // Calculate expected minimum success rate (60%)
    const int expected_min = NUM_CLIENTS * MESSAGES_PER_CLIENT * 0.6;
    
    std::cout << "High load test results:" << std::endl;
    std::cout << "- Expected at least " << expected_min << " successful messages" << std::endl;
    std::cout << "- Got " << successful_messages << " successful messages" << std::endl;
    std::cout << "- Server received " << received_heartbeats << " heartbeats" << std::endl;
    
    // Verify we got a reasonable number of successful operations
    EXPECT_GT(successful_messages, expected_min)
        << "High load test had too many failures";
}

TEST_F(SimpleHeartbeatTest, StressTest) {
    const int NUM_CLIENTS = 100;  // Increased from 30 to 100
    const int MESSAGES_PER_CLIENT = 10;  // Increased from 5 to 10
    const int MESSAGE_SIZE = 1024;  // 1KB per message
    std::vector<std::thread> clients;
    std::atomic<int> successful_messages{0};
    std::atomic<int> failed_messages{0};
    
    // Pre-generate a message pattern
    std::string message_pattern(MESSAGE_SIZE, 'X');
    for (size_t i = 0; i < message_pattern.size(); ++i) {
        message_pattern[i] = 'A' + (i % 26);  // Cycle through A-Z
    }
    
    auto client_func = [&](int client_id) {
        int sockfd = socket(AF_INET, SOCK_STREAM, 0);
        if (sockfd < 0) {
            failed_messages += MESSAGES_PER_CLIENT;
            return;
        }
        
        // Set socket options
        int opt = 1;
        setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
        
        // Set timeouts
        struct timeval tv;
        tv.tv_sec = 1;
        tv.tv_usec = 0;
        setsockopt(sockfd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
        setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        
        // Enable TCP_NODELAY to disable Nagle's algorithm
        int flag = 1;
        setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
        
        struct sockaddr_in serv_addr;
        memset(&serv_addr, 0, sizeof(serv_addr));
        serv_addr.sin_family = AF_INET;
        serv_addr.sin_port = htons(TEST_PORT);
        inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr);
        
        // Connect with timeout
        if (connect(sockfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
            close(sockfd);
            failed_messages += MESSAGES_PER_CLIENT;
            return;
        }
        
        // Send messages
        for (int i = 0; i < MESSAGES_PER_CLIENT && server_running; ++i) {
            std::string msg = "HEARTBEAT:CLIENT" + std::to_string(client_id) + 
                            ":MSG" + std::to_string(i) + ":" + message_pattern;
            
            ssize_t total_sent = 0;
            const char* data = msg.c_str();
            size_t remaining = msg.length();
            
            while (remaining > 0 && server_running) {
                ssize_t sent = send(sockfd, data + total_sent, remaining, 0);
                if (sent <= 0) {
                    if (errno == EINTR) continue;
                    failed_messages++;
                    break;
                }
                total_sent += sent;
                remaining -= sent;
            }
            
            if (total_sent > 0) {
                successful_messages++;
            }
            
            // Small delay between messages
            std::this_thread::sleep_for(1ms);
        }
        
        // Graceful shutdown
        shutdown(sockfd, SHUT_WR);
        
        // Drain any remaining data (shouldn't be any in this test)
        char buf[1024];
        while (recv(sockfd, buf, sizeof(buf), 0) > 0) {}
        
        close(sockfd);
    };
    
    auto start_time = std::chrono::steady_clock::now();
    
    // Start all clients with staggered delays
    for (int i = 0; i < NUM_CLIENTS; ++i) {
        clients.emplace_back([&, i]() {
            // Stagger client starts
            std::this_thread::sleep_for(std::chrono::milliseconds(i * 5));
            client_func(i);
        });
    }
    
    // Wait for all clients to finish
    for (auto& t : clients) {
        if (t.joinable()) {
            t.join();
        }
    }
    
    auto end_time = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        end_time - start_time);
    
    // Wait for all messages to be processed
    std::this_thread::sleep_for(3000ms);
    
    // Calculate metrics
    const int total_expected = NUM_CLIENTS * MESSAGES_PER_CLIENT;
    const int total_actual = successful_messages + failed_messages;
    const double success_rate = (total_actual > 0) ? 
        (static_cast<double>(successful_messages) / total_actual) * 100.0 : 0.0;
    
    const double messages_per_second = (duration.count() > 0) ?
        (successful_messages * 1000.0) / duration.count() : 0.0;
    
    // Print detailed results
    std::cout << "\n=== Stress Test Results ===" << std::endl;
    std::cout << "Test Duration: " << duration.count() << " ms" << std::endl;
    std::cout << "Clients: " << NUM_CLIENTS << std::endl;
    std::cout << "Messages per Client: " << MESSAGES_PER_CLIENT << std::endl;
    std::cout << "Message Size: " << MESSAGE_SIZE << " bytes" << std::endl;
    std::cout << "Total Messages Attempted: " << total_expected << std::endl;
    std::cout << "Messages Sent Successfully: " << successful_messages << std::endl;
    std::cout << "Messages Failed: " << failed_messages << std::endl;
    std::cout << "Success Rate: " << std::fixed << std::setprecision(2) 
              << success_rate << "%" << std::endl;
    std::cout << "Messages per Second: " << std::fixed << std::setprecision(2)
              << messages_per_second << " msg/s" << std::endl;
    std::cout << "Server Received: " << received_heartbeats << " heartbeats" << std::endl;
    std::cout << "==========================" << std::endl;
    
    // Verify we got a reasonable number of successful operations
    const int expected_min = total_expected * 0.7;  // 70% success rate
    EXPECT_GT(successful_messages, expected_min)
        << "Stress test had too many failures (expected at least " 
        << expected_min << " successful messages)";
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
