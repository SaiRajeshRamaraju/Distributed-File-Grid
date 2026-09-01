#!/bin/bash

# Distributed File Grid - Service Status Script
# This script checks the status of all components

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PIDS_DIR="$PROJECT_ROOT/pids"

echo -e "${BLUE}=== Distributed File Grid Status ===${NC}"
echo ""

# Function to check service status
check_service() {
    local service_name="$1"
    local pid_file="$PIDS_DIR/${service_name}.pid"
    
    printf "%-20s: " "$service_name"
    
    if [ ! -f "$pid_file" ]; then
        echo -e "${RED}Not running (no PID file)${NC}"
        return 1
    fi
    
    local pid=$(cat "$pid_file")
    
    if kill -0 "$pid" 2>/dev/null; then
        echo -e "${GREEN}Running (PID: $pid)${NC}"
        return 0
    else
        echo -e "${RED}Not running (stale PID: $pid)${NC}"
        rm -f "$pid_file"
        return 1
    fi
}

# Check Redis
printf "%-20s: " "Redis"
if command -v redis-cli >/dev/null 2>&1 && redis-cli ping >/dev/null 2>&1; then
    echo -e "${GREEN}Running${NC}"
else
    echo -e "${RED}Not running${NC}"
fi

# Check all services
check_service "health_checker"
check_service "head_server"
check_service "cluster_server_1"
check_service "cluster_server_2"
check_service "cluster_server_3"

echo ""
echo "Log files location: $PROJECT_ROOT/logs/"
echo "PID files location: $PIDS_DIR/"
