#include "tcp_test_server.hpp"
#include <algorithm>
#include <gtest/gtest.h>

TEST(SimpleHeartbeatTest, ReceivesExactPayload) {
  TcpTestServer server;
  int client = dfg::net::connect_with_timeout("127.0.0.1", server.port(), 2);
  ASSERT_GE(client, 0);
  std::string payload = "test heartbeat";
  EXPECT_TRUE(dfg::net::send_all(client, payload.data(), payload.size()));
  ::close(client);
  ASSERT_TRUE(server.wait_for_messages(1));
  EXPECT_EQ(server.messages().front(), std::vector<char>(payload.begin(), payload.end()));
}

TEST(SimpleHeartbeatTest, ReceivesLargeBinaryPayloadWithoutTruncation) {
  TcpTestServer server;
  int client = dfg::net::connect_with_timeout("127.0.0.1", server.port(), 2);
  ASSERT_GE(client, 0);
  std::vector<char> payload(1024 * 1024);
  for (size_t i = 0; i < payload.size(); ++i) payload[i] = static_cast<char>(i % 256);
  EXPECT_TRUE(dfg::net::send_all(client, payload.data(), payload.size()));
  ::close(client);
  ASSERT_TRUE(server.wait_for_messages(1));
  EXPECT_EQ(server.messages().front(), payload);
}

TEST(SimpleHeartbeatTest, ReceivesEveryRapidConnection) {
  TcpTestServer server;
  for (int i = 0; i < 100; ++i) {
    int client = dfg::net::connect_with_timeout("127.0.0.1", server.port(), 2);
    ASSERT_GE(client, 0);
    EXPECT_TRUE(dfg::net::send_all(client, &i, sizeof(i)));
    ::close(client);
  }
  ASSERT_TRUE(server.wait_for_messages(100));
  auto messages = server.messages();
  ASSERT_EQ(messages.size(), 100u);
  for (int i = 0; i < 100; ++i) {
    ASSERT_EQ(messages[i].size(), sizeof(i));
    int received = -1;
    std::memcpy(&received, messages[i].data(), sizeof(received));
    EXPECT_EQ(received, i);
  }
}

TEST(SimpleHeartbeatTest, ReassemblesPartialWrites) {
  TcpTestServer server;
  int client = dfg::net::connect_with_timeout("127.0.0.1", server.port(), 2);
  ASSERT_GE(client, 0);
  std::vector<char> payload(4096, 'x');
  for (size_t offset = 0; offset < payload.size(); offset += 17) {
    EXPECT_TRUE(dfg::net::send_all(client, payload.data() + offset,
                                 std::min(size_t{17}, payload.size() - offset)));
  }
  ::close(client);
  ASSERT_TRUE(server.wait_for_messages(1));
  EXPECT_EQ(server.messages().front(), payload);
}

TEST(SimpleHeartbeatTest, HandlesConcurrentClients) {
  TcpTestServer server;
  std::atomic<int> failures{0};
  std::vector<std::thread> clients;
  for (int i = 0; i < 20; ++i) {
    clients.emplace_back([&] {
      int client = dfg::net::connect_with_timeout("127.0.0.1", server.port(), 2);
      if (client < 0) { ++failures; return; }
      std::vector<char> payload(4096, 'x');
      if (!dfg::net::send_all(client, payload.data(), payload.size())) ++failures;
      ::close(client);
    });
  }
  for (auto& client : clients) client.join();
  EXPECT_EQ(failures.load(), 0);
  ASSERT_TRUE(server.wait_for_messages(20));
  for (const auto& payload : server.messages()) {
    EXPECT_EQ(payload, std::vector<char>(4096, 'x'));
  }
}

TEST(SimpleHeartbeatTest, RejectsConnectionToBoundButNotListeningSocket) {
  int reserved = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  ASSERT_GE(reserved, 0);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  int bound = ::bind(reserved, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
  EXPECT_EQ(bound, 0);
  socklen_t len = sizeof(addr);
  int named = ::getsockname(reserved, reinterpret_cast<sockaddr*>(&addr), &len);
  EXPECT_EQ(named, 0);
  if (bound == 0 && named == 0) {
    int client = dfg::net::connect_with_timeout("127.0.0.1", ntohs(addr.sin_port), 2);
    EXPECT_LT(client, 0);
    if (client >= 0) ::close(client);
  }
  ::close(reserved);
}
