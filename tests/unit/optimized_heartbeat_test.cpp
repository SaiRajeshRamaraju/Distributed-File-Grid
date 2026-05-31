#include "../src/Cluster_Server/optimized_heartbeat_server.hpp"
#include <gtest/gtest.h>
#include <thread>
#include <vector>
#include <memory>
#include <random>
#include <chrono>
#include <iomanip>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <atomic>

using namespace std::chrono_literals;

class OptimizedHeartbeatTest : public ::testing::Test {
protected:
    static constexpr int TEST_PORT = 9002;  // Different port to avoid conflicts
    std::unique_ptr<OptimizedHeartbeatServer> server;
    std::thread server_thread;
    
    void SetUp() override {
        // Start the server in a separate thread
        server = std::make_unique<OptimizedHeartbeatServer>(TEST_PORT, 4);  // 4 worker threads
        server_thread = std::thread([this] { server->start(); });
        
        // Give the server time to start
        std::this_thread::sleep_for(100ms);
    }
    
    void TearDown() override {
        if (server) {
            server->stop();
        }
        if (server_thread.joinable()) {
            server_thread.join();
        }
    }
    
    // Helper function to create a test client
    static bool create_test_client(const std::string& message, int port = TEST_PORT) {
        int sockfd = socket(AF_INET, SOCK_STREAM, 0);
        if (sockfd < 0) {
            return false;
        }
        
        struct sockaddr_in serv_addr;
        memset(&serv_addr, 0, sizeof(serv_addr));
        serv_addr.sin_family = AF_INET;
        serv_addr.sin_port = htons(port);
        inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr);
        
        // Set connection timeout
        struct timeval tv;
        tv.tv_sec = 1;
        tv.tv_usec = 0;
        setsockopt(sockfd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
        
        if (connect(sockfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
            close(sockfd);
            return false;
        }
        
        // Send the message
        ssize_t sent = send(sockfd, message.c_str(), message.length(), 0);
        
        // Cleanup
        shutdown(sockfd, SHUT_WR);
        close(sockfd);
        
        return sent > 0;
    }
};

TEST_F(OptimizedHeartbeatTest, BasicConnectionTest) {
    EXPECT_TRUE(create_test_client("HEARTBEAT:TEST"));
    
    // Give the server a moment to process
    std::this_thread::sleep_for(100ms);
    
    auto metrics = server->get_metrics();
    EXPECT_GE(metrics.total_received_messages, 1);
}

TEST_F(OptimizedHeartbeatTest, MultipleClientsTest) {
    const int NUM_CLIENTS = 100;
    std::vector<std::thread> clients;
    std::atomic<int> successful_clients{0};
    
    auto client_func = [&](int id) {
        std::string msg = "HEARTBEAT:CLIENT" + std::to_string(id);
        if (create_test_client(msg)) {
            successful_clients++;
        }
    };
    
    // Start all clients
    for (int i = 0; i < NUM_CLIENTS; ++i) {
        clients.emplace_back(client_func, i);
    }
    
    // Wait for all clients to finish
    for (auto& t : clients) {
        if (t.joinable()) {
            t.join();
        }
    }
    
    // Give the server time to process all messages
    std::this_thread::sleep_for(500ms);
    
    auto metrics = server->get_metrics();
    std::cout << "\n=== Multiple Clients Test Results ===" << std::endl;
    std::cout << "Successful clients: " << successful_clients << "/" << NUM_CLIENTS << std::endl;
    std::cout << "Messages received: " << metrics.total_received_messages << std::endl;
    std::cout << "Bytes received: " << metrics.total_bytes_received << std::endl;
    std::cout << "Average processing time: " 
              << static_cast<double>(metrics.total_processing_time_ns.load()) / (metrics.total_received_messages > 0 ? metrics.total_received_messages.load() : 1) 
              << " ns/message" << std::endl;
    
    EXPECT_GE(successful_clients, NUM_CLIENTS * 0.95);  // At least 95% success rate
}

TEST_F(OptimizedHeartbeatTest, HighLoadTest) {
    const int NUM_CLIENTS = 200;
    const int MESSAGES_PER_CLIENT = 10;
    const int MESSAGE_SIZE = 1024;  // 1KB
    
    std::vector<std::thread> clients;
    std::atomic<int> successful_messages{0};
    
    // Generate a random message pattern
    std::string message_pattern;
    message_pattern.reserve(MESSAGE_SIZE);
    for (int i = 0; i < MESSAGE_SIZE; ++i) {
        message_pattern.push_back('A' + (i % 26));
    }
    
    auto client_func = [&](int client_id) {
        int sockfd = socket(AF_INET, SOCK_STREAM, 0);
        if (sockfd < 0) {
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
        
        // Connect to server
        struct sockaddr_in serv_addr;
        memset(&serv_addr, 0, sizeof(serv_addr));
        serv_addr.sin_family = AF_INET;
        serv_addr.sin_port = htons(TEST_PORT);
        inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr);
        
        if (connect(sockfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
            close(sockfd);
            return;
        }
        
        // Send multiple messages
        for (int i = 0; i < MESSAGES_PER_CLIENT; ++i) {
            std::string msg = "HEARTBEAT:CLIENT" + std::to_string(client_id) + 
                            ":MSG" + std::to_string(i) + ":" + message_pattern;
            
            ssize_t sent = send(sockfd, msg.c_str(), msg.length(), 0);
            if (sent > 0) {
                successful_messages++;
            }
            
            // Small delay between messages
            std::this_thread::sleep_for(1ms);
        }
        
        // Cleanup
        shutdown(sockfd, SHUT_WR);
        close(sockfd);
    };
    
    auto start_time = std::chrono::steady_clock::now();
    
    // Start all clients with staggered delays
    for (int i = 0; i < NUM_CLIENTS; ++i) {
        clients.emplace_back(client_func, i);
        std::this_thread::sleep_for(1ms);  // Stagger client starts
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
    
    // Give the server time to process all messages
    std::this_thread::sleep_for(1000ms);
    
    auto metrics = server->get_metrics();
    
    // Calculate metrics
    double messages_per_second = (successful_messages * 1000.0) / duration.count();
    double avg_message_size = (metrics.total_bytes_received.load() * 1.0) / 
                             (metrics.total_received_messages > 0 ? metrics.total_received_messages.load() : 1);
    
    // Print results
    std::cout << "\n=== High Load Test Results ===" << std::endl;
    std::cout << "Test Duration: " << duration.count() << " ms" << std::endl;
    std::cout << "Clients: " << NUM_CLIENTS << std::endl;
    std::cout << "Messages per Client: " << MESSAGES_PER_CLIENT << std::endl;
    std::cout << "Message Size: " << MESSAGE_SIZE << " bytes" << std::endl;
    std::cout << "Total Messages Attempted: " << (NUM_CLIENTS * MESSAGES_PER_CLIENT) << std::endl;
    std::cout << "Messages Sent Successfully: " << successful_messages << std::endl;
    std::cout << "Success Rate: " << std::fixed << std::setprecision(2)
              << ((successful_messages * 100.0) / (NUM_CLIENTS * MESSAGES_PER_CLIENT)) 
              << "%" << std::endl;
    std::cout << "Messages per Second: " << std::fixed << std::setprecision(2)
              << messages_per_second << " msg/s" << std::endl;
    std::cout << "Throughput: " << std::fixed << std::setprecision(2)
              << (messages_per_second * avg_message_size / (1024 * 1024)) 
              << " MB/s" << std::endl;
    std::cout << "Server Metrics:" << std::endl;
    std::cout << "- Total Received Messages: " << metrics.total_received_messages << std::endl;
    std::cout << "- Total Bytes Received: " << metrics.total_bytes_received << std::endl;
    std::cout << "- Total Clients Connected: " << metrics.total_clients_connected << std::endl;
    std::cout << "- Average Processing Time: " 
              << (metrics.total_processing_time_ns.load() / (metrics.total_received_messages > 0 ? metrics.total_received_messages.load() : 1)) 
              << " ns/message" << std::endl;
    
    // Verify we got a reasonable number of successful operations
    const int expected_min = (NUM_CLIENTS * MESSAGES_PER_CLIENT) * 0.9;  // 90% success rate
    EXPECT_GE(successful_messages, expected_min)
        << "High load test had too many failures (expected at least " 
        << expected_min << " successful messages)";
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
