#!/bin/bash

# Distributed File Grid - Service Startup Script
set -e

echo "=== Starting Distributed File Grid Services ==="

# Function to wait for service to be ready
wait_for_service() {
    local host=$1
    local port=$2
    local service_name=$3
    local max_attempts=30
    local attempt=1
    
    echo "Waiting for $service_name to be ready at $host:$port..."
    
    while [ $attempt -le $max_attempts ]; do
        if nc -z "$host" "$port" 2>/dev/null; then
            echo "$service_name is ready!"
            return 0
        fi
        
        echo "Attempt $attempt/$max_attempts: $service_name not ready yet..."
        sleep 2
        attempt=$((attempt + 1))
    done
    
    echo "ERROR: $service_name failed to start within timeout"
    return 1
}

# Function to start service in background
start_service() {
    local service_name=$1
    local command=$2
    local log_file="/app/logs/${service_name}.log"
    
    echo "Starting $service_name..."
    mkdir -p /app/logs
    
    # Start service and redirect output to log file
    nohup $command > "$log_file" 2>&1 &
    local pid=$!
    
    echo "$service_name started with PID $pid"
    echo $pid > "/tmp/${service_name}.pid"
    
    # Give service a moment to start
    sleep 2
    
    # Check if process is still running
    if ! kill -0 $pid 2>/dev/null; then
        echo "ERROR: $service_name failed to start"
        cat "$log_file"
        return 1
    fi
    
    return 0
}

# Create necessary directories
mkdir -p /app/logs /app/data /tmp/chunks /tmp/cluster_storage

# Set environment variables with defaults
export SERVER_ID=${SERVER_ID:-"server_1"}
export SERVER_PORT=${SERVER_PORT:-9669}
export REDIS_HOST=${REDIS_HOST:-"localhost"}
export REDIS_PORT=${REDIS_PORT:-6379}
export ZK_HOSTS=${ZK_HOSTS:-"localhost:2181"}
export METRICS_PORT=${METRICS_PORT:-9091}

echo "Configuration:"
echo "  SERVER_ID: $SERVER_ID"
echo "  SERVER_PORT: $SERVER_PORT"
echo "  REDIS_HOST: $REDIS_HOST"
echo "  REDIS_PORT: $REDIS_PORT"
echo "  ZK_HOSTS: $ZK_HOSTS"
echo "  METRICS_PORT: $METRICS_PORT"

# Start Redis if not external
if [ "$REDIS_HOST" = "localhost" ] || [ "$REDIS_HOST" = "127.0.0.1" ]; then
    echo "Starting Redis server..."
    redis-server --daemonize yes --port $REDIS_PORT --logfile /app/logs/redis.log
    wait_for_service localhost $REDIS_PORT "Redis"
fi

# Start ZooKeeper if not external
if [[ "$ZK_HOSTS" == *"localhost"* ]] || [[ "$ZK_HOSTS" == *"127.0.0.1"* ]]; then
    echo "Starting ZooKeeper..."
    cd /opt/zookeeper
    ./bin/zkServer.sh start
    wait_for_service localhost 2181 "ZooKeeper"
fi

# Determine which services to start based on available executables and environment
if [ -f "/usr/local/bin/head_server" ]; then
    echo "Head Server executable found"
    
    # Wait for dependencies
    if [ "$REDIS_HOST" != "localhost" ] && [ "$REDIS_HOST" != "127.0.0.1" ]; then
        wait_for_service "$REDIS_HOST" "$REDIS_PORT" "External Redis"
    fi
    
    # Start head server
    start_service "head_server" "/usr/local/bin/head_server"
    
    # Wait for head server to be ready
    wait_for_service localhost $SERVER_PORT "Head Server"
fi

if [ -f "/usr/local/bin/cluster_server" ]; then
    echo "Cluster Server executable found"
    
    # Parse server ID and port from environment or use defaults
    CLUSTER_SERVER_ID=${SERVER_ID##*_}  # Extract number from server_id
    CLUSTER_PORT=${SERVER_PORT:-8080}
    
    # Start cluster server
    start_service "cluster_server" "/usr/local/bin/cluster_server --server-id $CLUSTER_SERVER_ID --port $CLUSTER_PORT"
    
    # Wait for cluster server to be ready
    wait_for_service localhost $CLUSTER_PORT "Cluster Server"
fi

if [ -f "/usr/local/bin/health_checker" ]; then
    echo "Health Checker executable found"
    
    # Start health checker
    start_service "health_checker" "/usr/local/bin/health_checker"
    
    # Wait for health checker to be ready
    wait_for_service localhost 9000 "Health Checker"
fi

if [ -f "/usr/local/bin/zk_head_server_monitor" ]; then
    echo "ZooKeeper Head Server Monitor executable found"
    
    # Wait for ZooKeeper
    ZK_HOST=$(echo $ZK_HOSTS | cut -d: -f1)
    ZK_PORT=$(echo $ZK_HOSTS | cut -d: -f2)
    wait_for_service "$ZK_HOST" "$ZK_PORT" "ZooKeeper"
    
    # Start ZooKeeper monitor
    start_service "zk_monitor" "/usr/local/bin/zk_head_server_monitor --zk-hosts $ZK_HOSTS"
fi

echo "=== All services started successfully ==="

# Function to handle shutdown
shutdown_services() {
    echo "Shutting down services..."
    
    # Kill all background processes
    for pid_file in /tmp/*.pid; do
        if [ -f "$pid_file" ]; then
            local pid=$(cat "$pid_file")
            local service_name=$(basename "$pid_file" .pid)
            
            echo "Stopping $service_name (PID: $pid)..."
            if kill -TERM "$pid" 2>/dev/null; then
                # Wait for graceful shutdown
                local count=0
                while kill -0 "$pid" 2>/dev/null && [ $count -lt 10 ]; do
                    sleep 1
                    count=$((count + 1))
                done
                
                # Force kill if still running
                if kill -0 "$pid" 2>/dev/null; then
                    echo "Force killing $service_name..."
                    kill -KILL "$pid" 2>/dev/null || true
                fi
            fi
            
            rm -f "$pid_file"
        fi
    done
    
    echo "All services stopped"
    exit 0
}

# Set up signal handlers
trap shutdown_services SIGTERM SIGINT

# Keep the script running and monitor services
echo "Services are running. Press Ctrl+C to stop."
echo "Logs are available in /app/logs/"

# Monitor loop
while true do
    sleep 30
    
    # Check if all services are still running
    all_running=true
    for pid_file in /tmp/*.pid; do
        if [ -f "$pid_file" ]; then
            local pid=$(cat "$pid_file")
            local service_name=$(basename "$pid_file" .pid)
            
            if ! kill -0 "$pid" 2>/dev/null; then
                echo "WARNING: $service_name (PID: $pid) has stopped unexpectedly"
                all_running=false
            fi
        fi
    done
    
    if [ "$all_running" = false ]; then
        echo "Some services have failed. Check logs in /app/logs/"
    fi
done