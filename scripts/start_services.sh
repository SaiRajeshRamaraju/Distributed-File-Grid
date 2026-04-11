#!/bin/bash

# Distributed File Grid - Service Startup Script
# This script starts all components of the distributed file storage system

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="$PROJECT_ROOT/build"
LOGS_DIR="$PROJECT_ROOT/logs"
PIDS_DIR="$PROJECT_ROOT/pids"

# Create necessary directories
mkdir -p "$LOGS_DIR" "$PIDS_DIR" "/tmp/chunks" "/tmp/cluster_storage"

echo -e "${BLUE}=== Distributed File Grid Startup ===${NC}"
echo "Project root: $PROJECT_ROOT"
echo "Build directory: $BUILD_DIR"
echo "Logs directory: $LOGS_DIR"
echo ""

# Function to start a service
start_service() {
    local service_name="$1"
    local executable="$2"
    local args="$3"
    local log_file="$LOGS_DIR/${service_name}.log"
    local pid_file="$PIDS_DIR/${service_name}.pid"
    
    echo -e "${YELLOW}Starting $service_name...${NC}"
    
    if [ -f "$pid_file" ]; then
        local old_pid=$(cat "$pid_file")
        if kill -0 "$old_pid" 2>/dev/null; then
            echo -e "${YELLOW}$service_name is already running (PID: $old_pid)${NC}"
            return 0
        else
            rm -f "$pid_file"
        fi
    fi
    
    # Start the service in background
    nohup "$executable" $args > "$log_file" 2>&1 &
    local pid=$!
    echo $pid > "$pid_file"
    
    # Wait a moment and check if it's still running
    sleep 2
    if kill -0 "$pid" 2>/dev/null; then
        echo -e "${GREEN}$service_name started successfully (PID: $pid)${NC}"
        echo "  Log file: $log_file"
        return 0
    else
        echo -e "${RED}Failed to start $service_name${NC}"
        echo "  Check log file: $log_file"
        rm -f "$pid_file"
        return 1
    fi
}

# Function to check if Redis is running
check_redis() {
    if command -v redis-cli >/dev/null 2>&1; then
        if redis-cli ping >/dev/null 2>&1; then
            echo -e "${GREEN}Redis is already running${NC}"
            return 0
        fi
    fi
    
    echo -e "${YELLOW}Starting Redis server...${NC}"
    if command -v redis-server >/dev/null 2>&1; then
        nohup redis-server > "$LOGS_DIR/redis.log" 2>&1 &
        local redis_pid=$!
        echo $redis_pid > "$PIDS_DIR/redis.pid"
        
        # Wait for Redis to start
        for i in {1..10}; do
            if redis-cli ping >/dev/null 2>&1; then
                echo -e "${GREEN}Redis started successfully (PID: $redis_pid)${NC}"
                return 0
            fi
            sleep 1
        done
        
        echo -e "${RED}Failed to start Redis${NC}"
        return 1
    else
        echo -e "${RED}Redis server not found. Please install Redis.${NC}"
        return 1
    fi
}

# Build the project if needed
if [ ! -d "$BUILD_DIR" ] || [ ! -f "$BUILD_DIR/head_server" ]; then
    echo -e "${YELLOW}Building project...${NC}"
    cd "$PROJECT_ROOT"
    mkdir -p build
    cd build
    cmake ..
    make -j$(nproc)
    cd "$PROJECT_ROOT"
    echo -e "${GREEN}Build completed${NC}"
    echo ""
fi

# Check if executables exist
if [ ! -f "$BUILD_DIR/head_server" ] || [ ! -f "$BUILD_DIR/cluster_server" ] || [ ! -f "$BUILD_DIR/health_checker" ]; then
    echo -e "${RED}Error: Required executables not found in build directory${NC}"
    echo "Please run 'make' to build the project first."
    exit 1
fi

# Start Redis (optional - system works with on-disk metadata if Redis is unavailable)
check_redis || echo -e "${YELLOW}Redis not available - using on-disk metadata backend${NC}"
echo ""

# Start Health Checker
start_service "health_checker" "$BUILD_DIR/health_checker" "" || exit 1
echo ""

# Start Head Server
start_service "head_server" "$BUILD_DIR/head_server" "" || exit 1
echo ""

# Start Cluster Servers
start_service "cluster_server_1" "$BUILD_DIR/cluster_server" "--server-id 1 --port 8080" || exit 1
start_service "cluster_server_2" "$BUILD_DIR/cluster_server" "--server-id 2 --port 8081" || exit 1
start_service "cluster_server_3" "$BUILD_DIR/cluster_server" "--server-id 3 --port 8082" || exit 1
echo ""

echo -e "${GREEN}=== All services started successfully! ===${NC}"
echo ""
echo "Service status:"
echo "  Health Checker:     port 9000  (metrics: http://localhost:9096/metrics)"
echo "  Head Server:        port 9669  (metrics: http://localhost:9095/metrics)"
echo "  Cluster Server 1:   port 8080  (metrics: http://localhost:9091/metrics)"
echo "  Cluster Server 2:   port 8081"
echo "  Cluster Server 3:   port 8082"
echo ""
echo "Logs are available in: $LOGS_DIR"
echo "PID files are in: $PIDS_DIR"
echo ""
echo "To upload a file:    cd $BUILD_DIR && ./main upload <filepath> <name>"
echo "To download a file:  cd $BUILD_DIR && ./main download <name> <output>"
echo "To list files:       cd $BUILD_DIR && ./main list"
echo ""
echo "To stop all services, run: ./scripts/stop_services.sh"
echo "To check service status, run: ./scripts/status.sh"
