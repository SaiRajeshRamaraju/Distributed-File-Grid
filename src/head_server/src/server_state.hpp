#pragma once

#include <dfg/health_monitor.hpp>
#include <dfg/server_registry.hpp>
#include <memory>

extern std::unique_ptr<dfg::HealthMonitor> g_health_monitor;
extern std::unique_ptr<dfg::ServerRegistry> g_cluster_registry;
