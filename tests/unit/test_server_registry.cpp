#include <dfg/server_registry.hpp>
#include <dfg/health_monitor.hpp>
#include <gtest/gtest.h>

TEST(ServerRegistryTest, DeduplicatesAddressesAndPreservesTransferPorts) {
  dfg::ServerRegistry registry;
  int id = registry.add_server("127.0.0.1", 8080, 8180, 8280);
  EXPECT_EQ(registry.add_server("127.0.0.1", 8080, 8180, 8280), id);
  EXPECT_EQ(registry.count(), 1);
  auto entries = registry.get_all();
  ASSERT_EQ(entries.size(), 1u);
  EXPECT_EQ(entries[0].transfer_port, 8180);
  EXPECT_EQ(entries[0].public_port, 8280);
  EXPECT_EQ(registry.get_addresses(), std::vector<std::string>{"127.0.0.1:8080"});
}

TEST(ServerRegistryTest, ReportsMembershipChangesAndDoesNotReuseIds) {
  dfg::ServerRegistry registry;
  std::vector<std::pair<int, bool>> changes;
  registry.set_change_callback([&](const dfg::ServerEntry& entry, bool added) {
    changes.emplace_back(entry.id, added);
  });
  int first = registry.add_server("127.0.0.1", 8080);
  EXPECT_TRUE(registry.remove_server(first));
  EXPECT_FALSE(registry.remove_server(first));
  int second = registry.add_server("127.0.0.1", 8081);
  EXPECT_GT(second, first);
  EXPECT_EQ(registry.count(), 1);
  EXPECT_EQ(changes, (std::vector<std::pair<int, bool>>{
      {first, true}, {first, false}, {second, true}}));
}

TEST(HealthMonitorTest, TriggersFailureOnceAndRecoversOnHeartbeat) {
  // Negative timeout makes records immediately stale without wall-clock sleeps.
  dfg::HealthMonitor monitor(-1, 2);
  monitor.register_server(7, "127.0.0.1:8080");
  int failures = 0;
  int recoveries = 0;
  monitor.set_unhealthy_callback([&](int id, const dfg::ServerHealth& health) {
    EXPECT_EQ(id, 7);
    EXPECT_FALSE(health.is_healthy);
    ++failures;
    EXPECT_EQ(monitor.healthy_count(), 0); // callbacks must run outside the lock
  });
  monitor.set_recovered_callback([&](int id, const dfg::ServerHealth& health) {
    EXPECT_EQ(id, 7);
    EXPECT_TRUE(health.is_healthy);
    ++recoveries;
    EXPECT_EQ(monitor.healthy_count(), 1);
  });
  monitor.check_health();
  EXPECT_EQ(failures, 0);
  monitor.check_health();
  monitor.check_health();
  EXPECT_EQ(failures, 1);
  dfg::ServerHealth heartbeat;
  heartbeat.server_id = 7;
  heartbeat.ip = "127.0.0.1:8080";
  monitor.record_heartbeat(7, heartbeat);
  monitor.record_heartbeat(7, heartbeat);
  EXPECT_EQ(recoveries, 1);
  EXPECT_EQ(monitor.get_all_health().at(7).missed_heartbeats, 0);
  monitor.deregister_server(7);
  EXPECT_EQ(monitor.total_count(), 0);
}
