#include <gtest/gtest.h>
#include <dfg/sha256.hpp>
#include <dfg/net_utils.hpp>
#include <dfg/system_info.hpp>
#include <fstream>
#include <vector>
#include <string>
#include <filesystem>
#include <chrono>

namespace fs = std::filesystem;

class E2EIntegrityTest : public ::testing::Test {
protected:
    std::string test_dir;
    std::string sample_file;
    std::string expected_hash;

    void SetUp() override {
        test_dir = "/tmp/dfg_e2e_gtest_" + std::to_string(std::chrono::system_clock::now().time_since_epoch().count());
        fs::create_directories(test_dir);
        sample_file = test_dir + "/sample.dat";

        // Generate 128KB test payload
        std::ofstream out(sample_file, std::ios::binary);
        std::vector<char> data(128 * 1024, 'X');
        for (size_t i = 0; i < data.size(); ++i) {
            data[i] = static_cast<char>((i % 256));
        }
        out.write(data.data(), data.size());
        out.close();

        expected_hash = dfg::hash::sha256(data);
    }

    void TearDown() override {
        fs::remove_all(test_dir);
    }
};

TEST_F(E2EIntegrityTest, VerifySha256Digest) {
    std::ifstream in(sample_file, std::ios::binary);
    ASSERT_TRUE(in.is_open());
    std::vector<char> buffer((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    in.close();

    std::string actual_hash = dfg::hash::sha256(buffer);
    EXPECT_EQ(actual_hash, expected_hash);
    EXPECT_FALSE(actual_hash.empty());
    EXPECT_EQ(actual_hash.length(), 64U);
}

TEST_F(E2EIntegrityTest, MultiMegabyteDigest) {
    // Generate 2MB payload
    const size_t two_mb = 2 * 1024 * 1024;
    std::vector<char> large_data(two_mb);
    for (size_t i = 0; i < two_mb; ++i) {
        large_data[i] = static_cast<char>((i * 31 + 17) % 256);
    }

    std::string hash1 = dfg::hash::sha256(large_data);
    std::string hash2 = dfg::hash::sha256(large_data);

    EXPECT_EQ(hash1, hash2);
    EXPECT_EQ(hash1.length(), 64U);
    EXPECT_FALSE(hash1.empty());
}

TEST_F(E2EIntegrityTest, EmptyFileDigest) {
    std::string empty_file = test_dir + "/empty.dat";
    std::ofstream out(empty_file, std::ios::binary);
    out.close();

    std::vector<char> empty_data;
    std::string empty_hash = dfg::hash::sha256(empty_data);
    // SHA256 of empty string is e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
    EXPECT_EQ(empty_hash, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
}

TEST_F(E2EIntegrityTest, ChunkBoundarySizes) {
    // Test 1-byte, 64-byte block boundary, 1KB, and 64KB chunk boundaries
    std::vector<size_t> test_sizes = {1, 63, 64, 65, 1024, 65535, 65536, 65537};
    for (size_t sz : test_sizes) {
        std::vector<char> buf(sz, 'B');
        std::string h = dfg::hash::sha256(buf);
        EXPECT_EQ(h.length(), 64U);
        EXPECT_FALSE(h.empty());
    }
}

TEST_F(E2EIntegrityTest, CorruptedDataDetection) {
    std::vector<char> data(4096, 'A');
    std::string original_hash = dfg::hash::sha256(data);

    // Corrupt a single bit/byte
    data[2048] = 'Z';
    std::string corrupted_hash = dfg::hash::sha256(data);

    EXPECT_NE(original_hash, corrupted_hash);
    EXPECT_EQ(corrupted_hash.length(), 64U);
}

TEST_F(E2EIntegrityTest, NetworkAddressParsing) {
    std::string ip;
    int port = 0;

    // Standard host:port
    dfg::net::parse_address("127.0.0.1:9669", ip, port, 8080);
    EXPECT_EQ(ip, "127.0.0.1");
    EXPECT_EQ(port, 9669);

    // Host without port -> uses default port
    dfg::net::parse_address("192.168.1.50", ip, port, 8080);
    EXPECT_EQ(ip, "192.168.1.50");
    EXPECT_EQ(port, 8080);

    // Named host:port
    dfg::net::parse_address("head-server-1:9670", ip, port, 9669);
    EXPECT_EQ(ip, "head-server-1");
    EXPECT_EQ(port, 9670);
}

TEST_F(E2EIntegrityTest, SystemMetricsTelemetry) {
    auto [diskGB, diskPct] = getDiskUsageGBPercent();
    auto [ramGB, ramPct] = getRamUsageGBPercent();

    EXPECT_GE(diskGB, 0.0f);
    EXPECT_GE(diskPct, 0.0f);
    EXPECT_LE(diskPct, 100.0f);

    EXPECT_GE(ramGB, 0.0f);
    EXPECT_GE(ramPct, 0.0f);
    EXPECT_LE(ramPct, 100.0f);
}

TEST_F(E2EIntegrityTest, CachedSystemMonitorTelemetry) {
    auto& monitor = CachedSystemMonitor::instance();
    monitor.sample();
    EXPECT_TRUE(monitor.has_sample());

    auto usage = monitor.get();
    EXPECT_GE(usage.disk_usage, 0.0f);
    EXPECT_LE(usage.disk_usage, 100.0f);
    EXPECT_GE(usage.ram_usage, 0.0f);
    EXPECT_LE(usage.ram_usage, 100.0f);
}
