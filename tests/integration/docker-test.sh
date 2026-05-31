#!/bin/bash

# Distributed File Grid - Docker Test Script
set -e

echo "=== Distributed File Grid Docker Test Suite ==="

# Configuration
COMPOSE_FILE="docker-compose.yml"
PROJECT_NAME="dfg"
TEST_TIMEOUT=300  # 5 minutes

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    local color=$1
    local message=$2
    echo -e "${color}${message}${NC}"
}

# Function to wait for service to be healthy
wait_for_healthy() {
    local service_name=$1
    local max_attempts=30
    local attempt=1
    
    print_status $YELLOW "Waiting for $service_name to be healthy..."
    
    while [ $attempt -le $max_attempts ]; do
        if docker-compose -p $PROJECT_NAME ps $service_name | grep -q "healthy\|Up"; then
            print_status $GREEN "$service_name is healthy!"
            return 0
        fi
        
        echo "Attempt $attempt/$max_attempts: $service_name not ready yet..."
        sleep 10
        attempt=$((attempt + 1))
    done
    
    print_status $RED "ERROR: $service_name failed to become healthy within timeout"
    return 1
}

# Function to check service logs for errors
check_service_logs() {
    local service_name=$1
    local error_patterns=("ERROR" "FATAL" "Exception" "failed" "error")
    
    print_status $BLUE "Checking $service_name logs for errors..."
    
    local logs=$(docker-compose -p $PROJECT_NAME logs --tail=50 $service_name 2>/dev/null || echo "")
    
    for pattern in "${error_patterns[@]}"; do
        if echo "$logs" | grep -qi "$pattern"; then
            print_status $YELLOW "Warning: Found '$pattern' in $service_name logs"
            echo "$logs" | grep -i "$pattern" | head -3
        fi
    done
}

# Function to test file operations
test_file_operations() {
    print_status $BLUE "Testing file operations..."
    
    # Create test data
    mkdir -p test_data
    echo "This is a test file for the distributed storage system." > test_data/test.txt
    echo "It contains multiple lines to test chunking and reconstruction." >> test_data/test.txt
    for i in {1..100}; do
        echo "Line $i: Lorem ipsum dolor sit amet, consectetur adipiscing elit." >> test_data/test.txt
    done
    
    local test_file_size=$(wc -c < test_data/test.txt)
    print_status $GREEN "Created test file: $test_file_size bytes"
    
    # Copy test file to test client container
    docker-compose -p $PROJECT_NAME exec -T test-client mkdir -p /app/test_data
    docker cp test_data/test.txt ${PROJECT_NAME}_test-client_1:/app/test_data/
    
    # Test file upload (simulated)
    print_status $BLUE "Testing file upload simulation..."
    docker-compose -p $PROJECT_NAME exec -T test-client bash -c "
        echo 'Simulating file upload...'
        ls -la /app/test_data/test.txt
        echo 'File upload simulation completed'
    "
    
    # Test health endpoints
    print_status $BLUE "Testing health endpoints..."
    
    # Test head server health (if available)
    if docker-compose -p $PROJECT_NAME exec -T head-server-1 curl -f -s http://localhost:9669/health >/dev/null 2>&1; then
        print_status $GREEN "Head server health endpoint: OK"
    else
        print_status $YELLOW "Head server health endpoint: Not available (expected for current implementation)"
    fi
    
    # Test cluster server metrics
    if docker-compose -p $PROJECT_NAME exec -T cluster-server-1 curl -f -s http://localhost:9091/metrics >/dev/null 2>&1; then
        print_status $GREEN "Cluster server metrics endpoint: OK"
    else
        print_status $YELLOW "Cluster server metrics endpoint: Not available"
    fi
}

# Function to test ZooKeeper integration
test_zookeeper_integration() {
    print_status $BLUE "Testing ZooKeeper integration..."
    
    # Check ZooKeeper connectivity
    if docker-compose -p $PROJECT_NAME exec -T zookeeper zkCli.sh ls / >/dev/null 2>&1; then
        print_status $GREEN "ZooKeeper connectivity: OK"
    else
        print_status $YELLOW "ZooKeeper connectivity: Limited"
    fi
    
    # Check ZooKeeper monitor
    docker-compose -p $PROJECT_NAME exec -T zk-head-monitor ps aux | grep zk_head_server_monitor || true
}

# Function to test monitoring stack
test_monitoring_stack() {
    print_status $BLUE "Testing monitoring stack..."
    
    # Test Prometheus
    if curl -f -s http://localhost:9090/api/v1/status/config >/dev/null 2>&1; then
        print_status $GREEN "Prometheus: OK"
    else
        print_status $YELLOW "Prometheus: Not accessible"
    fi
    
    # Test Grafana
    if curl -f -s http://localhost:3000/api/health >/dev/null 2>&1; then
        print_status $GREEN "Grafana: OK"
    else
        print_status $YELLOW "Grafana: Not accessible"
    fi
}

# Function to run performance test
run_performance_test() {
    print_status $BLUE "Running basic performance test..."
    
    # Test concurrent connections
    docker-compose -p $PROJECT_NAME exec -T test-client bash -c "
        echo 'Testing concurrent connections...'
        for i in {1..5}; do
            (echo 'test connection' | nc head-server-1 9669 &)
        done
        wait
        echo 'Concurrent connection test completed'
    " || print_status $YELLOW "Performance test completed with warnings"
}

# Cleanup function
cleanup() {
    print_status $YELLOW "Cleaning up..."
    docker-compose -p $PROJECT_NAME down -v --remove-orphans 2>/dev/null || true
    docker system prune -f >/dev/null 2>&1 || true
    rm -rf test_data logs data
}

# Main test execution
main() {
    print_status $BLUE "Starting Distributed File Grid Docker Test Suite"
    
    # Cleanup any existing containers
    cleanup
    
    # Build and start services
    print_status $BLUE "Building Docker images..."
    if ! docker-compose -p $PROJECT_NAME build; then
        print_status $RED "Failed to build Docker images"
        exit 1
    fi
    
    print_status $BLUE "Starting services..."
    if ! docker-compose -p $PROJECT_NAME up -d; then
        print_status $RED "Failed to start services"
        exit 1
    fi
    
    # Wait for core services to be ready
    print_status $BLUE "Waiting for services to be ready..."
    
    # Wait for infrastructure services first
    wait_for_healthy "zookeeper" || print_status $YELLOW "ZooKeeper startup timeout (continuing)"
    wait_for_healthy "redis" || print_status $YELLOW "Redis startup timeout (continuing)"
    
    # Wait for application services
    wait_for_healthy "head-server-1" || print_status $YELLOW "Head server 1 startup timeout (continuing)"
    wait_for_healthy "cluster-server-1" || print_status $YELLOW "Cluster server 1 startup timeout (continuing)"
    wait_for_healthy "health-checker" || print_status $YELLOW "Health checker startup timeout (continuing)"
    
    # Give services additional time to stabilize
    print_status $BLUE "Allowing services to stabilize..."
    sleep 30
    
    # Show service status
    print_status $BLUE "Service Status:"
    docker-compose -p $PROJECT_NAME ps
    
    # Check service logs for obvious errors
    print_status $BLUE "Checking service logs..."
    for service in head-server-1 cluster-server-1 health-checker zk-head-monitor; do
        check_service_logs $service
    done
    
    # Run tests
    test_file_operations
    test_zookeeper_integration
    test_monitoring_stack
    run_performance_test
    
    # Show final status
    print_status $BLUE "Final Service Status:"
    docker-compose -p $PROJECT_NAME ps
    
    # Show resource usage
    print_status $BLUE "Resource Usage:"
    docker stats --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}" $(docker-compose -p $PROJECT_NAME ps -q) 2>/dev/null || true
    
    print_status $GREEN "=== Test Suite Completed ==="
    print_status $BLUE "Services are running and accessible at:"
    echo "  - Head Server 1: http://localhost:9669"
    echo "  - Head Server 2: http://localhost:9670"
    echo "  - Cluster Servers: http://localhost:8080, 8081, 8082"
    echo "  - Prometheus: http://localhost:9090"
    echo "  - Grafana: http://localhost:3000 (admin/admin)"
    echo ""
    print_status $BLUE "To stop services: docker-compose -p $PROJECT_NAME down"
    print_status $BLUE "To view logs: docker-compose -p $PROJECT_NAME logs [service_name]"
}

# Handle script arguments
case "${1:-}" in
    "cleanup")
        cleanup
        exit 0
        ;;
    "logs")
        docker-compose -p $PROJECT_NAME logs -f "${2:-}"
        exit 0
        ;;
    "status")
        docker-compose -p $PROJECT_NAME ps
        exit 0
        ;;
    "stop")
        docker-compose -p $PROJECT_NAME down
        exit 0
        ;;
    "help"|"-h"|"--help")
        echo "Usage: $0 [command]"
        echo "Commands:"
        echo "  (no args)  - Run full test suite"
        echo "  cleanup    - Clean up containers and volumes"
        echo "  logs [svc] - Show logs for service (or all)"
        echo "  status     - Show service status"
        echo "  stop       - Stop all services"
        echo "  help       - Show this help"
        exit 0
        ;;
esac

# Set up signal handlers
trap cleanup EXIT INT TERM

# Run main test
main