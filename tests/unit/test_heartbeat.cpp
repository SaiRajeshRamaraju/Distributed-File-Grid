#include "tcp_test_server.hpp"
#include <dfg/async_net.hpp>
#include <gtest/gtest.h>

TEST(HeartbeatTest, SendsCompleteLengthPrefixedProtobuf) {
  TcpTestServer server;
  ASSERT_EQ(async_hb::send_signal("127.0.0.1", 123, server.port(),
                                "127.0.0.1:8080"), 0);
  ASSERT_TRUE(server.wait_for_messages(1));
  auto frame = server.messages().front();
  ASSERT_GE(frame.size(), 4u);
  uint32_t length = 0;
  std::memcpy(&length, frame.data(), sizeof(length));
  ASSERT_EQ(ntohl(length), frame.size() - 4);
  heart_beat::v1::HeartBeat heartbeat;
  ASSERT_TRUE(heartbeat.ParseFromArray(frame.data() + 4, frame.size() - 4));
  EXPECT_EQ(heartbeat.server_id(), 123);
  EXPECT_EQ(heartbeat.ip(), "127.0.0.1:8080");
  EXPECT_GT(heartbeat.timestamp().seconds(), 0);
}

TEST(HeartbeatTest, SendsDistinctServerIdsAcrossConnections) {
  TcpTestServer server;
  for (int i = 0; i < 5; ++i) {
    ASSERT_EQ(async_hb::send_signal("127.0.0.1", 100 + i, server.port()), 0);
  }
  ASSERT_TRUE(server.wait_for_messages(5));
  auto messages = server.messages();
  ASSERT_EQ(messages.size(), 5u);
  for (size_t i = 0; i < messages.size(); ++i) {
    ASSERT_GT(messages[i].size(), 4u);
    heart_beat::v1::HeartBeat heartbeat;
    ASSERT_TRUE(heartbeat.ParseFromArray(messages[i].data() + 4,
                                        messages[i].size() - 4));
    EXPECT_EQ(heartbeat.server_id(), 100 + i);
  }
}

TEST(HeartbeatTest, RejectsInvalidServerAddress) {
  EXPECT_NE(async_hb::send_signal("256.256.256.256", 123, 9000), 0);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
