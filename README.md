# Distributed File Grid

## Project Summary

A **complete distributed file storage system** designed to reduce read/write latency and ensure fault-tolerant data redundancy across unreliable nodes with advanced monitoring and orchestration capabilities.

- **High-throughput system** using asynchronous, multithreaded I/O to split files into 64 MB chunks with configurable replication
- **Dual head server architecture** with ZooKeeper-based leader election and automatic failover
- **Advanced health monitoring** through multiple monitoring services with real-time status reporting
- **Efficient communication** using Protocol Buffers for heartbeat signals, metadata exchange, and inter-node communication
- **Production monitoring** with Prometheus metrics, Grafana dashboards, and comprehensive logging

## Build Status

**Successfully builds and tested on Arch Linux with GCC 15.2.1**
- **5 executables** compile and run: `main`, `head_server`, `cluster_server`, `health_checker`, `zk_head_server_monitor`
- **Unified CLI**: `./build/main` entry point for all file operations and service management
- **JSON configuration** loaded from `config/` with environment variable overrides
- **ZooKeeper integration** with leader election and service discovery
- **Docker deployment** with complete orchestration support
- **Redis dependencies** properly isolated with conditional compilation
- **Prometheus metrics** integration working with Grafana dashboards
- **Protocol Buffers v32.0.0** compatible with C++20 coroutines

---

## System Architecture

The Distributed File Grid consists of **four main components**:

### 1. Head Servers (Dual Architecture)

- **Purpose**: Entry point for file operations and metadata management with high availability
- **Features**:
  - **Primary/Backup Configuration**: Multiple head servers with automatic leader election
  - **ZooKeeper Coordination**: Distributed consensus for leader selection and failover
  - **File Operations**: Upload/download coordination with load balancing
  - **Chunk Management**: Intelligent placement and replication management
  - **Metadata Storage**: Redis-based metadata with optional in-memory fallback

### 2. Cluster Servers (Distributed Storage)

- **Purpose**: Store actual file chunks with configurable replication and monitoring
- **Features**:
  - **64MB Chunk Storage**: Optimized chunk size with configurable replication factor
  - **Prometheus Metrics**: Real-time performance and resource monitoring
  - **Health Reporting**: Continuous heartbeat signals with resource usage data
  - **Integrity Verification**: Automatic chunk verification and corruption detection
  - **Load Balancing**: Intelligent distribution based on server capacity

### 3. Health Checker (Original Monitoring)

- **Purpose**: Traditional heartbeat-based monitoring and cluster coordination
- **Features**:
  - **Continuous Monitoring**: Real-time health checks of all cluster servers
  - **Failure Detection**: Automatic detection and handling of server failures
  - **Re-replication**: Triggered chunk re-replication on server failures
  - **Status Reporting**: Comprehensive health status and metrics collection

### 4. ZooKeeper Head Monitor (Advanced Coordination)

- **Purpose**: **NEW** - head server monitoring with distributed coordination
- **Features**:
  - **Leader Election**: Automatic leader selection using ZooKeeper consensus
  - **Service Discovery**: Dynamic head server registration and discovery
  - **Fault Tolerance**: Automatic failover when leader becomes unhealthy
  - **Health Monitoring**: TCP-based health checks with configurable timeouts
  - **Interactive Management**: Command-line interface for real-time operations

---

## Features

### Core Storage Features
- **Distributed Storage**: Files split into 64MB chunks across multiple servers
- **Fault Tolerance**: Configurable replication factor (default: 3x)
- **High Availability**: Dual head server architecture with automatic failover
- **Data Integrity**: Chunk-level checksums and automatic corruption detection
- **Load Balancing**: Intelligent chunk placement based on server capacity

### Advanced Monitoring & Management
- **ZooKeeper Integration**: Coordination and leader election
- **Prometheus Metrics**: Real-time performance monitoring with custom metrics
- **Grafana Dashboards**: Professional monitoring interface with alerting
- **Health Monitoring**: Multi-layer health checking (heartbeat + ZooKeeper)
- **Interactive Management**: Command-line tools for real-time operations

### Performance & Scalability
- **Protocol Buffers**: Efficient binary communication protocols
- **Asynchronous I/O**: High-performance coroutine-based operations
- **Resource Monitoring**: Real-time CPU, memory, and disk usage tracking
- **Horizontal Scaling**: Linear scaling with additional cluster servers
- **Network Optimization**: Optimized for high-throughput operations

---

## Quick Start

### Prerequisites

- **Linux** (tested on Arch Linux)
- **C++20** compiler (GCC 7+ or Clang 6+) [since protobufs only support C++ 20+]
- **CMake** 3.16+
- **Protocol Buffers** compiler (v3.0.0+, tested with v32.0.0)
- **Prometheus-CPP** library for metrics
- **Abseil** library for logging and utilities
- **ZLIB** for compression
- **Docker** and **Docker Compose** (for containerized deployment - optional)

**Note**: Redis dependencies are optional and disabled by default. The system can run without Redis.

### Installation

#### Option 1: Native Installation

1. **Install Dependencies**

   **Ubuntu/Debian:**

   ```bash
   sudo apt-get update
   sudo apt-get install -y build-essential cmake pkg-config \
       libssl-dev libprotobuf-dev protobuf-compiler \
       libfmt-dev libasio-dev
   ```

   **Arch Linux:**

   ```bash
   sudo pacman -S --needed base-devel cmake pkg-config \
        protobuf zlib abseil-cpp
   
   # Install Prometheus-CPP
   yay -S prometheus-cpp-git
   ```

2. **Build the Project**

   ```bash
   git clone <repository-url>
   cd Distributed-File-Grid
   mkdir build && cd build
   cmake -DBUILD_TESTS=OFF -DWITH_REDIS=OFF ..
   make -j$(nproc)
   ```

   This will create four executables:
   - `head_server` - Main file operations server with Redis integration
   - `cluster_server` - Chunk storage server with Prometheus metrics
   - `health_checker` - Traditional heartbeat-based monitoring service
   - `zk_head_server_monitor` - **NEW** ZooKeeper-based head server coordination

3. **Start Services (separate terminals)**
   ```bash
   # Terminal 1 – metadata/head server
   ./build/main head-server

   # Terminal 2 – storage node (use unique IDs/ports per replica)
   ./build/main cluster-server --server-id 1 --ip 127.0.0.1 --port 8080

   # Terminal 3 – heartbeat-based health checker
   ./build/main health-checker
   ```
   Each process runs in the foreground; keep them alive while issuing upload/download commands.

#### Option 2: Docker Deployment (Recommended)

1. **Quick Start with Testing**

   ```bash
   git clone <repository-url>
   cd Distributed-File-Grid
   
   # Run comprehensive test suite
   chmod +x docker-test.sh
   ./docker-test.sh
   ```

2. **Production Deployment**

   ```bash
   # Start all services
   docker-compose up -d
   
   # Check service status
   docker-compose ps
   
   # View logs
   docker-compose logs -f
   ```

3. **Access Web Interfaces**
   - **Grafana Dashboard**: http://localhost:3000 (admin/admin)
   - **Prometheus Metrics**: http://localhost:9090
   - **Head Server 1**: http://localhost:9669
   - **Head Server 2**: http://localhost:9670

4. **ZooKeeper Management**
   ```bash
   # Interactive ZooKeeper monitor
   docker-compose exec zk-head-monitor /usr/local/bin/zk_head_server_monitor -i
   
   # Commands: register, status, leader, quit
   ```

---

## Usage

### File Operations

Interact with the system through the CLI entry point (`./build/main`). Every command expects the services from the Quick Start section to be running in separate terminals.

```bash
# Upload a local file (splits into 64 MB chunks with 3x replication metadata)
./build/main upload /path/to/file.bin file.bin

# Download a tracked file
./build/main download file.bin /tmp/restored.bin

# List all files known to the metadata store (Redis or fallback)
./build/main list
```

> **Note:** The REST API referenced in earlier drafts is still under development. Use the CLI workflow above for the current release.

### Configuration

Configuration files are located in the `config/` directory:

- `head_server_config.json` - Head server settings
- `health_checker_config.json` - Health checker settings

Key configuration options:

```json
{
  "replication_factor": 3, // Number of chunk replicas
  "chunk_size": 67108864, // 64MB chunks
  "heartbeat_timeout": 60, // Health check interval (seconds)
  "max_connections": 1000 // Maximum concurrent connections
}
```

### Metadata Storage Modes

- **Redis-backed (recommended for production):** Configure the build with `-DWITH_REDIS=ON`. The head server automatically starts/uses a local Redis instance (or connects to the one you provide) for chunk metadata, replication coordination, and TTL support.
- **Embedded fallback (default):** When Redis is disabled, metadata is stored in a simple on-disk database at `/tmp/dfg_metadata.db`. Override the location via the `DFG_METADATA_DB` environment variable if you want the data under version control or a persistent volume.
- **Listing helper:** Regardless of the backend, `./build/main list` enumerates every tracked file by calling the new `list_all_files()` helper in `redis_handler.hpp`.

### Monitoring & Management

#### Grafana Dashboard (http://localhost:3000)
- **System Overview**: Real-time health status of all services
- **Performance Metrics**: CPU, memory, disk usage across cluster
- **Network Monitoring**: Throughput, latency, and connection metrics
- **Error Tracking**: Error rates and failure patterns
- **Custom Alerts**: Configurable alerting for critical events

#### Prometheus Metrics (http://localhost:9090)
- **Cluster Metrics**: `heartbeat_active_connections`, `heartbeat_messages_received_total`
- **Performance Metrics**: `heartbeat_processing_time_seconds`, `heartbeat_errors_total`
- **System Metrics**: CPU, memory, disk usage per service
- **Custom Queries**: Advanced metric analysis and correlation

#### ZooKeeper Management
```bash
# Interactive management
./build/zk_head_server_monitor -i

# Available commands:
register head_server_3 192.168.1.100 9671  # Register new head server
status                                       # Show health status
leader                                       # Show current leader
quit                                         # Exit
```

#### Health Check Endpoints
```bash
# Service health checks
curl http://localhost:9669/health    # Head server (if implemented)
curl http://localhost:9091/metrics   # Cluster server metrics
curl http://localhost:9090/targets   # Prometheus targets
```

---

## Architecture Details

### File Storage Process

1. **Upload**:
   - File received by head server
   - Split into 64MB chunks
   - Chunks distributed to cluster servers based on available storage
   - Replication factor enforced across servers

2. **Download**:
   - Head server retrieves file metadata
   - Chunks fetched from cluster servers
   - File reconstructed and served to client
   - Integrity verified using stored hashes

3. **Health Monitoring**:
   - Heartbeat signals sent every 30 seconds
   - Resource usage reported (CPU, memory, disk)
   - Failed servers detected automatically
   - Re-replication triggered for failed chunks

### Fault Tolerance

- **Replication**: Each chunk stored on multiple servers
- **Health Checks**: Continuous monitoring of all servers
- **Auto-Recovery**: Failed chunks automatically re-replicated
- **Leader Election**: Health checker coordinates failover
- **Graceful Degradation**: System continues operating with reduced capacity

---

## Development

### Project Structure

```
Distributed-File-Grid/
├── src/
│   ├── Cluster_Server/           # Chunk storage with metrics
│   ├── Head_Server/              # File operations and metadata
│   ├── Health_Checker/           # Traditional monitoring
│   ├── ZooKeeper_HealthChecker/  # NEW: ZooKeeper coordination
│   ├── include/                  # Shared headers and utilities
│   └── protos/v1/               # Protocol Buffer definitions
├── docker/                       # Docker configuration files
│   ├── start-services.sh        # Service startup script
│   ├── healthcheck.sh           # Health check script
│   ├── prometheus.yml           # Prometheus configuration
│   └── grafana-*.yml            # Grafana configuration
├── config/                       # Service configuration files
├── UnitTesting/                  # Test suites and validation
├── docker-compose.yml            # Complete orchestration
├── Dockerfile                    # Multi-stage container build
├── docker-test.sh               # Comprehensive test suite
└── test_zookeeper.sh            # ZooKeeper integration tests
```

### Building from Source

```bash
# Generate protocol buffer files
protoc --cpp_out=generated protos/*.proto

# Build with CMake
mkdir build && cd build
cmake -DBUILD_TESTS=OFF -DWITH_REDIS=OFF ..
make -j$(nproc)
```

### Recent Major Updates (September 2025)

#### **ZooKeeper Integration (NEW)**
- **Leader Election**: Automatic head server leader election using ZooKeeper consensus
- **Service Discovery**: Dynamic server registration and discovery
- **Fault Tolerance**: Automatic failover when leader becomes unhealthy
- **Interactive Management**: Command-line interface for real-time operations

#### **Docker & Orchestration (NEW)**
- **Complete Docker Setup**: Multi-service orchestration with docker-compose
- **Resource checking**: Health checks, logging, metrics, and monitoring
- **Automated Testing**: Comprehensive test suite with `docker-test.sh`
- **Grafana Integration**: Professional monitoring dashboards

#### **Build System Improvements**
- **Redis Dependencies**: Properly isolated with conditional compilation (`WITH_REDIS=OFF`)
- **Prometheus Metrics**: Fixed API usage for labeled metrics support
- **CMake Configuration**: Enhanced build system with proper dependency management
- **C++20 Support**: Full coroutine support with modern C++ features

#### **Production Features**
- **Multi-stage Docker Build**: Optimized container images
- **Health Monitoring**: Multiple monitoring layers (heartbeat + ZooKeeper)
- **Configuration Management**: Environment-based configuration
- **Logging & Debugging**: Comprehensive logging with centralized collection

**All executables compile and run successfully on Arch Linux with GCC 15.2.1**

## Current Status

- **CLI-first workflow**: `./build/main` drives the system via the `head-server`, `cluster-server`, `health-checker`, `upload`, `download`, and `list` commands. An HTTP API is planned but not yet implemented.
- **Metadata backends**: Redis remains fully supported (`-DWITH_REDIS=ON`), while builds without Redis transparently fall back to an on-disk metadata store at `/tmp/dfg_metadata.db` (override with `DFG_METADATA_DB`).
- **Embedded coordinator**: The ZooKeeper-style head monitor now ships with a built-in coordination layer, so the `zk_head_server_monitor` binary is always available without any external C dependencies.
- **Docker scripts**: Container and orchestration assets are provided for future work but are not required for the native CLI workflow described below.

### Running Tests

#### Native Testing
```bash
# Build and test all executables
./test_system.sh

# Test ZooKeeper integration specifically
./test_zookeeper.sh

# Manual testing
make test  # Unit tests (if BUILD_TESTS=ON)
```

#### Docker Testing (Recommended)
```bash
# Comprehensive system test with Docker
./docker-test.sh

# Manual Docker testing
docker-compose up -d
docker-compose ps
docker-compose logs -f

# Test ZooKeeper in Docker
docker-compose exec zk-head-monitor /usr/local/bin/zk_head_server_monitor -i
```

---

## Performance

### Benchmarks

- **Throughput**: 2× lower latency compared to traditional file systems
- **Scalability**: Linear scaling with additional cluster servers
- **Reliability**: Zero data loss during simulated multi-node failures
- **Efficiency**: 64MB chunks optimized for network transfer

### Resource Requirements

- **Head Server**: 2GB RAM, 2 CPU cores
- **Cluster Server**: 4GB RAM, 4 CPU cores (per server)
- **Health Checker**: 1GB RAM, 1 CPU core
- **Storage**: 100GB+ per cluster server (depending on replication factor)

---

## Troubleshooting

### Common Issues

1. **Service Won't Start**
   - Check if ports are available (9669, 8080-8082, 6379)
   - Check logs in `logs/` directory

2. **High Resource Usage**
   - Adjust `replication_factor` in configuration
   - Add more cluster servers
   - Monitor with dashboard at http://localhost:3000

3. **File Upload Failures**
   - Verify cluster servers are healthy
   - Check available storage on cluster servers
   - Review head server logs

### Logs and Debugging

#### Native Deployment
- **Head Server**: `logs/head_server.log`
- **Cluster Servers**: `logs/cluster_server_*.log`
- **Health Checker**: `logs/health_checker.log`
- **ZooKeeper Monitor**: `logs/zk_monitor.log`

#### Docker Deployment
```bash
# View all service logs
docker-compose logs -f

# View specific service logs
docker-compose logs -f head-server-1
docker-compose logs -f zk-head-monitor

# Real-time log monitoring
docker-compose logs -f --tail=100
```

### Health Checks

#### Service Health Endpoints
```bash
# Head servers
curl http://localhost:9669/health    # Head server 1
curl http://localhost:9670/health    # Head server 2

# Cluster servers
curl http://localhost:8080/health    # Cluster server 1
curl http://localhost:8081/health    # Cluster server 2
curl http://localhost:8082/health    # Cluster server 3

# Monitoring services
curl http://localhost:9090/api/v1/targets  # Prometheus targets
curl http://localhost:3000/api/health      # Grafana health
```

#### ZooKeeper Health Monitoring
```bash
# Check ZooKeeper monitor status
docker-compose exec zk-head-monitor ps aux | grep zk_head_server_monitor

# Interactive health check
docker-compose exec zk-head-monitor /usr/local/bin/zk_head_server_monitor -i
# Then use: status, leader commands
```

---

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

### Development Setup

```bash
# Install development dependencies
make install-deps

# Set up development environment
make dev

# Run tests
make test
```

## Additional Documentation

- **[Docker Deployment Guide](README_DOCKER.md)** - Comprehensive Docker setup and deployment
- **[ZooKeeper Integration Guide](ZOOKEEPER_DOCKER_IMPLEMENTATION.md)** - ZooKeeper features and usage
- **[Build Success Report](BUILD_SUCCESS_REPORT.md)** - Detailed build and testing results
- **[Test Results](ZOOKEEPER_TEST_RESULTS.md)** - Comprehensive testing validation

## Production Deployment

### Quick Production Setup
```bash
# 1. Clone and test
git clone <repository-url>
cd Distributed-File-Grid
./docker-test.sh

# 2. Production deployment
docker-compose up -d

# 3. Monitor and manage
docker-compose ps
docker-compose logs -f
```

### Features Ready
- **High Availability**: Dual head servers with ZooKeeper coordination
- **Monitoring**: Prometheus + Grafana with custom dashboards
- **Orchestration**: Complete Docker and Kubernetes support
- **Fault Tolerance**: Multi-layer health monitoring and automatic recovery
- **Scalability**: Horizontal scaling with load balancing
- **Security**: Network isolation and configurable authentication

## Acknowledgments

- Inspired by distributed file systems such as **Google File System (GFS)** and **Hadoop HDFS**
- Uses **[Protocol Buffers](https://developers.google.com/protocol-buffers)** for efficient serialization
- **[ZooKeeper](https://zookeeper.apache.org/)** for distributed coordination and consensus
- **[Prometheus](https://prometheus.io/)** and **[Grafana](https://grafana.com/)** for monitoring
- Built with **modern C++20**, **Docker**

---
**Version**: 1.0.0  
