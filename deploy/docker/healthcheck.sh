#!/bin/bash

# Distributed File Grid - Health Check Script
set -e

# Configuration
HEAD_SERVER_PORT=${SERVER_PORT:-9669}
CLUSTER_SERVER_PORT=${SERVER_PORT:-8080}
HEALTH_CHECKER_PORT=9000
METRICS_PORT=${METRICS_PORT:-9091}

# Function to check if a service is responding
check_service() {
    local service_name=$1
    local port=$2
    local endpoint=${3:-""}
    
    if nc -z localhost "$port" 2>/dev/null; then
        if [ -n "$endpoint" ]; then
            # Try HTTP endpoint if specified
            if curl -f -s "http://localhost:$port$endpoint" >/dev/null 2>&1; then
                echo "$service_name: HEALTHY (HTTP)"
                return 0
            else
                echo "$service_name: DEGRADED (TCP OK, HTTP FAILED)"
                return 1
            fi
        else
            echo "$service_name: HEALTHY (TCP)"
            return 0
        fi
    else
        echo "$service_name: UNHEALTHY (NO CONNECTION)"
        return 1
    fi
}

# Function to check process by PID file
check_process() {
    local service_name=$1
    local pid_file="/tmp/${service_name}.pid"
    
    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file")
        if kill -0 "$pid" 2>/dev/null; then
            echo "$service_name process: RUNNING (PID: $pid)"
            return 0
        else
            echo "$service_name process: DEAD (PID file exists but process not found)"
            return 1
        fi
    else
        echo "$service_name process: UNKNOWN (no PID file)"
        return 1
    fi
}

# Main health check
echo "=== Distributed File Grid Health Check ==="
echo "Timestamp: $(date)"

overall_health=0

# Check Head Server
if [ -f "/usr/local/bin/head_server" ]; then
    if check_service "Head Server" "$HEAD_SERVER_PORT" "/health"; then
        check_process "head_server"
    else
        overall_health=1
    fi
fi

# Check Cluster Server
if [ -f "/usr/local/bin/cluster_server" ]; then
    if check_service "Cluster Server" "$CLUSTER_SERVER_PORT"; then
        check_process "cluster_server"
    else
        overall_health=1
    fi
    
    # Check metrics endpoint
    check_service "Cluster Metrics" "$METRICS_PORT" "/metrics"
fi

# Check Health Checker
if [ -f "/usr/local/bin/health_checker" ]; then
    if check_service "Health Checker" "$HEALTH_CHECKER_PORT"; then
        check_process "health_checker"
    else
        overall_health=1
    fi
fi

# Check ZooKeeper Monitor
if [ -f "/usr/local/bin/zk_head_server_monitor" ]; then
    check_process "zk_monitor"
fi

# Check external dependencies
echo ""
echo "=== External Dependencies ==="

# Check Redis
if [ -n "$REDIS_HOST" ]; then
    REDIS_PORT=${REDIS_PORT:-6379}
    if check_service "Redis" "$REDIS_PORT"; then
        # Try Redis ping
        if redis-cli -h "${REDIS_HOST:-localhost}" -p "$REDIS_PORT" ping >/dev/null 2>&1; then
            echo "Redis: PING OK"
        else
            echo "Redis: PING FAILED"
            overall_health=1
        fi
    else
        overall_health=1
    fi
fi

# Check ZooKeeper
if [ -n "$ZK_HOSTS" ]; then
    ZK_HOST=$(echo "$ZK_HOSTS" | cut -d: -f1)
    ZK_PORT=$(echo "$ZK_HOSTS" | cut -d: -f2)
    ZK_PORT=${ZK_PORT:-2181}
    
    check_service "ZooKeeper" "$ZK_PORT"
fi

# System resources check
echo ""
echo "=== System Resources ==="

# Check disk space
DISK_USAGE=$(df /tmp | tail -1 | awk '{print $5}' | sed 's/%//')
if [ "$DISK_USAGE" -gt 90 ]; then
    echo "Disk Usage: CRITICAL ($DISK_USAGE%)"
    overall_health=1
elif [ "$DISK_USAGE" -gt 80 ]; then
    echo "Disk Usage: WARNING ($DISK_USAGE%)"
else
    echo "Disk Usage: OK ($DISK_USAGE%)"
fi

# Check memory usage
if command -v free >/dev/null 2>&1; then
    MEMORY_USAGE=$(free | grep Mem | awk '{printf "%.0f", $3/$2 * 100.0}')
    if [ "$MEMORY_USAGE" -gt 90 ]; then
        echo "Memory Usage: CRITICAL ($MEMORY_USAGE%)"
        overall_health=1
    elif [ "$MEMORY_USAGE" -gt 80 ]; then
        echo "Memory Usage: WARNING ($MEMORY_USAGE%)"
    else
        echo "Memory Usage: OK ($MEMORY_USAGE%)"
    fi
fi

# Check load average
if [ -f /proc/loadavg ]; then
    LOAD_AVG=$(cat /proc/loadavg | cut -d' ' -f1)
    echo "Load Average: $LOAD_AVG"
fi

# Log file sizes check
echo ""
echo "=== Log Files ==="
if [ -d "/app/logs" ]; then
    for log_file in /app/logs/*.log; do
        if [ -f "$log_file" ]; then
            log_size=$(du -h "$log_file" | cut -f1)
            echo "$(basename "$log_file"): $log_size"
        fi
    done
fi

# Final health status
echo ""
echo "=== Overall Health Status ==="
if [ $overall_health -eq 0 ]; then
    echo "STATUS: HEALTHY ✅"
    exit 0
else
    echo "STATUS: UNHEALTHY ❌"
    exit 1
fi