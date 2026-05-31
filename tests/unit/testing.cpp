#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <vector>
#include <atomic>
#include "../src/include/heart_beat_signal.hpp"

using namespace std::chrono_literals;

class HeartbeatTest : public ::testing::Test {
protected:
    static constexpr int TEST_PORT = 9001; // Different from default port to avoid conflicts
    static constexpr int NUM_HEARTBEATS = 5;
    static constexpr int SERVER_ID = 123;
    
    void SetUp() override {
        // Start a simple echo server in a separate thread
        server_running = true;
        server_thread = std::thread([this] { run_echo_server(); });
        
        // Give the server time to start
        std::this_thread::sleep_for(100ms);
    }
    
    void TearDown() override {
        server_running = false;
        if (server_thread.joinable()) {
            server_thread.join();
        }
    }
    
    void run_echo_server() {
        int server_fd = socket(AF_INET6, SOCK_STREAM, 0);
        if (server_fd < 0) {
            perror("socket");
            return;
        }
        
        int opt = 1;
        if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
            perror("setsockopt");
            close(server_fd);
            return;
        }
        
        sockaddr_in6 addr{};
        addr.sin6_family = AF_INET6;
        addr.sin6_addr = in6addr_any;
        addr.sin6_port = htons(TEST_PORT);
        
        if (bind(server_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
            perror("bind");
            close(server_fd);
            return;
        }
        
        if (listen(server_fd, 5) < 0) {
            perror("listen");
            close(server_fd);
            return;
        }
        
        while (server_running) {
            fd_set read_fds;
            FD_ZERO(&read_fds);
            FD_SET(server_fd, &read_fds);
            
            timeval timeout{};
            timeout.tv_sec = 1;
            
            int activity = select(server_fd + 1, &read_fds, nullptr, nullptr, &timeout);
            
            if (activity < 0) {
                if (errno == EINTR) continue;
                perror("select");
                break;
            }
            
            if (activity > 0 && FD_ISSET(server_fd, &read_fds)) {
                sockaddr_in6 client_addr{};
                socklen_t client_len = sizeof(client_addr);
                
                int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &client_len);
                if (client_fd < 0) {
                    perror("accept");
                    continue;
                }
                
                // Handle client in a separate thread
                std::thread([this, client_fd] {
                    char buffer[1024];
                    ssize_t bytes_read;
                    
                    while ((bytes_read = recv(client_fd, buffer, sizeof(buffer), 0)) > 0) {
                        // Echo back the received data
                        send(client_fd, buffer, bytes_read, 0);
                    }
                    
                    close(client_fd);
                }).detach();
            }
        }
        
        close(server_fd);
    }
    
    std::thread server_thread;
    std::atomic<bool> server_running{false};
};

TEST_F(HeartbeatTest, TestBasicHeartbeat) {
    // Test basic heartbeat sending
    int result = send_signal("::1", SERVER_ID, TEST_PORT);
    ASSERT_EQ(result, 0) << "Failed to send heartbeat";
    
    // Add a small delay to ensure the server processes the message
    std::this_thread::sleep_for(100ms);
}

TEST_F(HeartbeatTest, TestMultipleHeartbeats) {
    // Test sending multiple heartbeats
    for (int i = 0; i < NUM_HEARTBEATS; ++i) {
        int result = send_signal("::1", SERVER_ID + i, TEST_PORT);
        ASSERT_EQ(result, 0) << "Failed to send heartbeat " << i;
        std::this_thread::sleep_for(50ms);
    }
}

TEST_F(HeartbeatTest, TestInvalidServer) {
    // Test with invalid port (port 1 is usually restricted)
    int result = send_signal("::1", SERVER_ID, 1);
    EXPECT_NE(result, 0) << "Expected send to fail on restricted port";
}

TEST_F(HeartbeatTest, TestConcurrentHeartbeats) {
    // Test concurrent heartbeat sending
    constexpr int NUM_THREADS = 10;
    constexpr int HEARTBEATS_PER_THREAD = 5;
    
    std::vector<std::thread> threads;
    std::atomic<int> success_count{0};
    
    for (int i = 0; i < NUM_THREADS; ++i) {
        threads.emplace_back([&, i] {
            for (int j = 0; j < HEARTBEATS_PER_THREAD; ++j) {
                int result = send_signal(
                    "::1", 
                    SERVER_ID + i * HEARTBEATS_PER_THREAD + j, 
                    TEST_PORT
                );
                if (result == 0) {
                    success_count++;
                }
                std::this_thread::sleep_for(10ms);
            }
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    EXPECT_EQ(success_count, NUM_THREADS * HEARTBEATS_PER_THREAD)
        << "Some heartbeats failed to send";
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
