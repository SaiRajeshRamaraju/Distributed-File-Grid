#include <dfg/net_utils.hpp>
#include <dfg/unique_fd.hpp>
#include <gtest/gtest.h>

#include <fcntl.h>
#include <thread>
#include <utility>

namespace {
class NetworkStreamTest : public ::testing::Test {
protected:
  void SetUp() override {
    int descriptors[2];
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, descriptors), 0);
    reader.reset(descriptors[0]);
    writer.reset(descriptors[1]);
  }
  dfg::UniqueFd reader;
  dfg::UniqueFd writer;
};

TEST_F(NetworkStreamTest, ReadsLineWithoutConsumingBinaryPayload) {
  const std::string message("UPLOAD file\r\n\0\xff", 15);
  ASSERT_TRUE(dfg::net::send_all(writer.get(), message.data(), message.size()));
  std::string line;
  ASSERT_TRUE(dfg::net::recv_line(reader.get(), line));
  EXPECT_EQ(line, "UPLOAD file");
  char payload[2];
  ASSERT_TRUE(dfg::net::recv_all(reader.get(), payload, sizeof(payload)));
  EXPECT_EQ(payload[0], '\0');
  EXPECT_EQ(static_cast<unsigned char>(payload[1]), 0xff);
}

TEST_F(NetworkStreamTest, RejectsIncompleteAndOversizedLines) {
  ASSERT_TRUE(dfg::net::send_all(writer.get(), "abcde", 5));
  writer.reset();
  std::string line;
  EXPECT_FALSE(dfg::net::recv_line(reader.get(), line, 4));
  EXPECT_FALSE(dfg::net::recv_line(reader.get(), line));
}

TEST_F(NetworkStreamTest, ReadsCommandDeliveredInSeparateWrites) {
  std::thread sender([&] {
    for (char byte : std::string("LIST\n"))
      EXPECT_TRUE(dfg::net::send_all(writer.get(), &byte, 1));
  });
  std::string line;
  EXPECT_TRUE(dfg::net::recv_line(reader.get(), line));
  sender.join();
  EXPECT_EQ(line, "LIST");
}

TEST_F(NetworkStreamTest, DescriptorOwnershipTransfersAndCloses) {
  int original = writer.get();
  dfg::UniqueFd moved(std::move(writer));
  EXPECT_EQ(writer.get(), -1);
  EXPECT_EQ(moved.get(), original);
  moved.reset();
  errno = 0;
  EXPECT_EQ(::fcntl(original, F_GETFD), -1);
  EXPECT_EQ(errno, EBADF);
}
} // namespace
