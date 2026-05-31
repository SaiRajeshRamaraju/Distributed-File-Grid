#include "./test_heartbeat.cpp"
#include <atomic>
#include <gtest/gtest.h>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

class HeartbeatTest : public ::testing::Test {
protected:
  static constexpr int TEST_PORT = 9001; // Different port to avoid conflicts
  std::atomic<bool> server_running{false};
  std::thread server_thread;
  std::atomic<int> received_heartbeats{0};
  std::string last_received_ip;
  int last_received_id = -1;

  void SetUp() override {
    // Start server in a separate thread
    server_running = true;
    server_thread = std::thread([this]() {
      int sfd = socket(AF_INET6, SOCK_STREAM | SOCK_NONBLOCK, 0);
      if (sfd < 0) {
        GTEST_FAIL() << "Failed to create server socket";
        return;
      }

      // Allow both IPv4 and IPv6
      int opt = 1;
      setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

      // Set up server address
      struct sockaddr_in6 addr;
      memset(&addr, 0, sizeof(addr));
      addr.sin6_family = AF_INET6;
      addr.sin6_addr = in6addr_any;
      addr.sin6_port = htons(TEST_PORT);

      if (bind(sfd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(sfd);
        GTEST_FAIL() << "Failed to bind server socket";
        return;
      }

      listen(sfd, 5);

      while (server_running) {
        struct sockaddr_in6 client_addr;
        socklen_t client_len = sizeof(client_addr);
        int client_fd =
            accept(sfd, (struct sockaddr *)&client_addr, &client_len);

        if (client_fd < 0) {
          if (errno == EAGAIN || errno == EWOULDBLOCK) {
            std::this_thread::sleep_for(10ms);
            continue;
          }
          GTEST_FAIL() << "Accept failed: " << strerror(errno);
          break;
        }

        // Handle client
        std::thread([this, client_fd]() {
          char buffer[1024];
          while (server_running) {
            ssize_t bytes = recv(client_fd, buffer, sizeof(buffer), 0);
            if (bytes <= 0)
              break;

            // Parse heartbeat
            heart_beat::v1::HeartBeat hb;
            if (hb.ParseFromArray(buffer, bytes)) {
              received_heartbeats++;
              last_received_ip = hb.ip();
              last_received_id = hb.server_id();

              // Process the heartbeat
              auto timestamp = std::chrono::system_clock::time_point(
                  std::chrono::seconds(hb.timestamp().seconds()) +
                  std::chrono::nanoseconds(hb.timestamp().nanos()));
              auto now = std::chrono::system_clock::now();
              auto latency =
                  std::chrono::duration_cast<std::chrono::milliseconds>(
                      now - timestamp)
                      .count();

              std::cout << "Received heartbeat from server_id: "
                        << hb.server_id() << ", IP: " << hb.ip()
                        << ", Latency: " << latency << "ms" << std::endl;
            }
          }
          close(client_fd);
        }).detach();
      }
      close(sfd);
    });

    // Wait for server to start
    std::this_thread::sleep_for(100ms);
  }

  void TearDown() override {
    server_running = false;
    if (server_thread.joinable()) {
      server_thread.join();
    }
  }
};

TEST_F(HeartbeatTest, TestHeartbeatSending) {
  // Send a heartbeat
  int result = send_signal("::1", 123, TEST_PORT);
  ASSERT_EQ(result, 0) << "Failed to send heartbeat";

  // Wait for the heartbeat to be received
  std::this_thread::sleep_for(200ms);

  // Verify the heartbeat was received
  EXPECT_GT(received_heartbeats, 0) << "No heartbeats received";
  EXPECT_EQ(last_received_id, 123) << "Incorrect server ID received";
}

TEST_F(HeartbeatTest, TestMultipleHeartbeats) {
  const int NUM_HEARTBEATS = 5;

  // Send multiple heartbeats
  for (int i = 0; i < NUM_HEARTBEATS; ++i) {
    int result = send_signal("::1", 100 + i, TEST_PORT);
    ASSERT_EQ(result, 0) << "Failed to send heartbeat " << i;
    std::this_thread::sleep_for(50ms);
  }

  // Wait for all heartbeats to be processed
  std::this_thread::sleep_for(500ms);

  // Verify all heartbeats were received
  EXPECT_GE(received_heartbeats, NUM_HEARTBEATS)
      << "Expected at least " << NUM_HEARTBEATS << " heartbeats, but got "
      << received_heartbeats;
}

TEST_F(HeartbeatTest, TestInvalidServer) {
  // Try to send to an invalid port
  int result = send_signal("::1", 123, 1); // Port 1 is usually restricted
  EXPECT_NE(result, 0) << "Expected send to fail on restricted port";
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
