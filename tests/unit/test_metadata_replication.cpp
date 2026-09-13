#include <gtest/gtest.h>
#include <dfg/health_monitor.hpp>
#include <dfg/server_registry.hpp>
#include <dfg/net_utils.hpp>
#include <dfg/async_net.hpp>
#include "../../src/head_server/redis_handler.hpp"
#include <thread>
#include <chrono>
#include <filesystem>

class MetadataReplicationTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Use a unique isolated database file for the test
        db_file_ = "/tmp/test_metadata_rep_" + std::to_string(getpid()) + ".db";
        setenv("DFG_METADATA_DB", db_file_.c_str(), 1);
        std::filesystem::remove(db_file_);
    }

    void TearDown() override {
        std::filesystem::remove(db_file_);
    }

    std::string db_file_;
};

TEST_F(MetadataReplicationTest, MetadataStoresReplicaCountAndPath) {
    std::string filename = "dataset.bin";
    std::string file_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    // Format: filename \n TTL \n HASH \n REPLICAS \n <cid> <srv> <path> <checksum> <replicas>
    std::stringstream req;
    req << filename << "\n";
    req << "TTL=3600\n";
    req << "HASH=" << file_hash << "\n";
    req << "REPLICAS=3\n";
    req << "0 127.0.0.1:8080 /var/storage/chunk_0.dat deadbeef0 3\n";
    req << "0 127.0.0.1:8081 /var/storage/chunk_0_rep1.dat deadbeef0 3\n";
    req << "0 127.0.0.1:8082 /var/storage/chunk_0_rep2.dat deadbeef0 3\n";
    req << "1 127.0.0.1:8080 /var/storage/chunk_1.dat deadbeef1 3\n";
    req << "1 127.0.0.1:8081 /var/storage/chunk_1_rep1.dat deadbeef1 3\n";
    req << "1 127.0.0.1:8082 /var/storage/chunk_1_rep2.dat deadbeef1 3\n";

    create_entry(req.str());

    auto rec = query_metadata(filename);
    EXPECT_EQ(rec.file_hash, file_hash);
    EXPECT_EQ(rec.replica_count, 3);
    ASSERT_EQ(rec.chunks.size(), 6u);

    // Verify chunk 0 path and server
    bool found_server80 = false;
    for (const auto& c : rec.chunks) {
        if (c.chunk_id == 0 && c.server == "127.0.0.1:8080") {
            found_server80 = true;
            EXPECT_EQ(c.path, "/var/storage/chunk_0.dat");
            EXPECT_EQ(c.replica_count, 3);
            EXPECT_EQ(c.checksum, "deadbeef0");
        }
    }
    EXPECT_TRUE(found_server80);
}

TEST_F(MetadataReplicationTest, AtomicServerReplacementAndDeletion) {
    std::string filename = "archive.tar";
    std::string file_hash = "abc123hash";

    std::stringstream req;
    req << filename << "\n";
    req << "HASH=" << file_hash << "\n";
    req << "REPLICAS=3\n";
    req << "0 127.0.0.1:8080 /path/c0_srv80.dat c0_chk 3\n";
    req << "0 127.0.0.1:8081 /path/c0_srv81.dat c0_chk 3\n";
    req << "0 127.0.0.1:8082 /path/c0_srv82.dat c0_chk 3\n";
    create_entry(req.str());

    // Assume 127.0.0.1:8080 died, replacement server is 127.0.0.1:8083
    std::string dead_server = "127.0.0.1:8080";
    std::string new_server = "127.0.0.1:8083";

    metadata_store::ChunkRecord new_chunk;
    new_chunk.chunk_id = 0;
    new_chunk.server = new_server;
    new_chunk.path = "/path/c0_srv83.dat";
    new_chunk.checksum = "c0_chk";
    new_chunk.replica_count = 3;

    std::map<std::string, std::vector<metadata_store::ChunkRecord>> updates;
    updates[filename].push_back(new_chunk);

    // Perform atomic replacement
    bool ok = atomic_replace_server_chunks(dead_server, new_server, updates);
    EXPECT_TRUE(ok);

    // Delete dead server metadata
    delete_server_metadata(dead_server);

    auto rec = query_metadata(filename);
    EXPECT_EQ(rec.replica_count, 3);

    bool has_dead = false;
    bool has_new = false;
    for (const auto& c : rec.chunks) {
        if (c.server == dead_server) has_dead = true;
        if (c.server == new_server) {
            has_new = true;
            EXPECT_EQ(c.path, "/path/c0_srv83.dat");
        }
    }
    EXPECT_FALSE(has_dead);
    EXPECT_TRUE(has_new);
}

TEST_F(MetadataReplicationTest, HealthMonitorTenMissedHeartbeats) {
    // Verify HealthMonitor triggers unhealthy callback at 10 missed heartbeats
    dfg::HealthMonitor monitor(1 /* 1s timeout */, 10 /* 10 missed max */);
    monitor.register_server(1, "127.0.0.1:8080");

    std::atomic<bool> unhealthy_triggered{false};
    monitor.set_unhealthy_callback([&](int sid, const dfg::ServerHealth& h) {
        if (sid == 1) {
            unhealthy_triggered = true;
        }
    });

    // Advance missed heartbeats by waiting and checking health
    std::this_thread::sleep_for(std::chrono::milliseconds(2100));

    // Call check_health 9 times: should still be healthy
    for (int i = 1; i <= 9; ++i) {
        monitor.check_health();
        EXPECT_FALSE(unhealthy_triggered);
    }

    // 10th check should reach max_missed and trigger callback
    monitor.check_health();
    EXPECT_TRUE(unhealthy_triggered);
}

TEST_F(MetadataReplicationTest, EmergencyTcpDeadSignal) {
    // Spin up a dummy TCP listener simulating cluster server emergency listener
    int sfd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    ASSERT_GE(sfd, 0);
    int yes = 1;
    setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(0); // auto-port
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    ASSERT_EQ(::bind(sfd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);
    ASSERT_EQ(::listen(sfd, 1), 0);

    socklen_t len = sizeof(addr);
    ASSERT_EQ(::getsockname(sfd, reinterpret_cast<sockaddr*>(&addr), &len), 0);
    int port = ntohs(addr.sin_port);

    std::atomic<bool> received_dead{false};
    std::thread server_thread([sfd, &received_dead]() {
        sockaddr_in peer{};
        socklen_t plen = sizeof(peer);
        int cfd = ::accept(sfd, reinterpret_cast<sockaddr*>(&peer), &plen);
        if (cfd >= 0) {
            char buf[64] = {0};
            ssize_t n = ::recv(cfd, buf, sizeof(buf) - 1, 0);
            if (n > 0) {
                std::string msg(buf, n);
                if (msg.find("DEAD") != std::string::npos) {
                    received_dead = true;
                    ::send(cfd, "OK\n", 3, 0);
                }
            }
            ::close(cfd);
        }
        ::close(sfd);
    });

    // Connect and send DEAD signal
    int client_sock = dfg::net::connect_with_timeout("127.0.0.1", port, 2);
    ASSERT_GE(client_sock, 0);
    std::string dead_cmd = "DEAD\n";
    EXPECT_TRUE(dfg::net::send_all(client_sock, dead_cmd.data(), dead_cmd.size()));

    char resp[64] = {0};
    ssize_t n = ::recv(client_sock, resp, sizeof(resp) - 1, 0);
    EXPECT_GT(n, 0);
    EXPECT_NE(std::string(resp).find("OK"), std::string::npos);
    ::close(client_sock);

    server_thread.join();
    EXPECT_TRUE(received_dead);
}
