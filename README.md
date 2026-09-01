# Distributed File Grid

A high-performance, fault-tolerant distributed file storage grid in modern **C++20**. Files are chunked into 64 MB blocks, replicated across storage nodes, and reassembled on demand with cryptographic integrity verification, consensus voting, and read-repair.

---

## What It Does

- **Chunking & Replication**: Splits files into configurable chunks (default: 64 MB), replicates each chunk across $N$ storage nodes (default: 3), and computes SHA-256 digests for end-to-end data integrity.
- **Consensus & Read Repair**: File downloads query chunk hashes across replicas, vote on consensus hashes, and automatically trigger background read-repairs if an out-of-sync or corrupted chunk is detected.
- **Dynamic Cluster Membership**: Cluster storage servers self-register dynamically with the Head Server's Control API (`:9670`).
- **Heartbeat & System Monitoring**: Storage nodes emit real-time system metrics (CPU, RAM, Disk, Network) to the Health Checker.
- **High Availability & Leader Election**: Integrated in-memory ZooKeeper client enables leader election and head server failover.
- **Observability**: Exposes native Prometheus metrics across all nodes and provides pre-configured Grafana dashboards with Loki log aggregation.

---

## Architecture

```
                       ┌────────────────────────────────────────────────────────┐
                       │                   User Applications                    │
                       │   ┌──────────────────┐          ┌──────────────────┐   │
                       │   │  dfg Unified CLI │          │ dfg_client / App │   │
                       │   └─────────┬────────┘          └────────┬─────────┘   │
                       └─────────────┼────────────────────────────┼─────────────┘
                                     │                            │
                     HTTP Control    │                            │ TCP Streaming
                     API (:9670)     │                            │ (:9669)
                                     ▼                            ▼
                       ┌────────────────────────────────────────────────────────┐
                       │                      Head Server                       │
                       │  - Metadata Store (Redis / On-Disk)                    │
                       │  - Chunk Placement Engine & Consensus Voting           │
                       │  - Control API (:9670) & Metrics Exporter (:9095)      │
                       └──────┬──────────────────────┬───────────────────┬──────┘
                              │                      │                   │
                     ┌────────┘                      │                   └────────┐
             TCP Chunk Transfers            TCP Chunk Transfers          TCP Chunk Transfers
             (:8180)                        (:8181)                      (:8182)
                     │                              │                            │
         ┌───────────▼───────────┐      ┌───────────▼───────────┐    ┌───────────▼───────────┐
         │   Cluster Server 1    │      │   Cluster Server 2    │    │   Cluster Server 3    │
         │   - Storage Engine    │      │   - Storage Engine    │    │   - Storage Engine    │
         │   - TCP Server :8080  │      │   - TCP Server :8081  │    │   - TCP Server :8082  │
         │   - Metrics :9091     │      │   - Metrics :9092     │    │   - Metrics :9093     │
         └───────────┬───────────┘      └───────────┬───────────┘    └───────────┬───────────┘
                     │                              │                            │
                     └──────────────────────┬───────┴────────────────────────────┘
                                     UDP Heartbeats
                                            │
                                 ┌──────────▼──────────┐
                                 │   Health Checker    │
                                 │   - UDP Receiver    │
                                 │   - Auto-Failover   │
                                 │   - Web UI :9098    │
                                 │   - Metrics :9096   │
                                 └──────────┬──────────┘
                                            │
                                 ┌──────────▼──────────┐
                                 │  ZooKeeper Monitor  │
                                 │  - Leader Election  │
                                 │  - Coordination     │
                                 └─────────────────────┘
```

---

## Binaries & Components

| Binary | Description |
|---|---|
| `dfg` | Unified CLI binary — handles file upload/download/list, cluster management, and starting embedded daemons. |
| `head_server` | Standalone metadata and coordination master. |
| `cluster_server` | Standalone storage node daemon for receiving and serving chunk data. |
| `health_checker` | Standalone health monitoring daemon with web dashboard and Prometheus metrics. |
| `zk_head_server_monitor` | ZooKeeper-based leader election and head server failover monitor. |
| `dfg_client` | Standalone dedicated client utility for file downloads. |

---

## Default Network Ports

| Service | Port | Protocol | Purpose |
|---|---|---|---|
| Head Server | `9669` | TCP | File download client socket |
| Head Server Control API | `9670` | HTTP / TCP | Dynamic cluster registration & management |
| Head Server Metrics | `9095` | HTTP / TCP | Prometheus exporter |
| Cluster Server 1 | `8080` / `8180` | TCP | Server socket / Chunk transfer socket |
| Cluster Server 1 Metrics | `9091` | HTTP / TCP | Prometheus exporter |
| Cluster Server 2 | `8081` / `8181` | TCP | Server socket / Chunk transfer socket |
| Cluster Server 2 Metrics | `9092` | HTTP / TCP | Prometheus exporter |
| Cluster Server 3 | `8082` / `8182` | TCP | Server socket / Chunk transfer socket |
| Cluster Server 3 Metrics | `9093` | HTTP / TCP | Prometheus exporter |
| Health Checker Heartbeat | `9000` | UDP / TCP | Heartbeat telemetry receiver |
| Health Checker Web UI | `9098` | HTTP / TCP | Live HTML status dashboard |
| Health Checker Metrics | `9096` | HTTP / TCP | Prometheus exporter |
| ZooKeeper Ensemble | `2181` | TCP | Coordination & leader election |
| Redis Metadata Backend | `6379` | TCP | Persistent metadata store (optional) |
| Prometheus | `9090` | HTTP / TCP | Metrics aggregator |
| Grafana | `3000` | HTTP / TCP | Visualization dashboards (admin / admin) |

---

## Getting Started

### Prerequisites

- **C++ Compiler**: GCC 13+ or Clang 16+ (supporting C++20 and coroutines)
- **Build System**: CMake 3.16+, Make
- **Libraries**: Protocol Buffers (`protobuf`, `protoc`), `zlib`
- **Optional**: `redis-server`, `redis-tools` (for Redis metadata persistence)

### System Dependencies

**Ubuntu / Debian**:
```bash
sudo apt-get update
sudo apt-get install -y build-essential cmake pkg-config \
    libprotobuf-dev protobuf-compiler \
    zlib1g-dev libfmt-dev redis-server redis-tools
```

**Arch Linux**:
```bash
sudo pacman -S --needed base-devel cmake pkg-config protobuf zlib fmt redis
```

---

## Build & Test

### Standard Build
```bash
git clone https://github.com/SaiRajeshRamaraju/Distributed-File-Grid.git
cd Distributed-File-Grid

mkdir build && cd build
cmake ..
make -j$(nproc)
```

### Build with Redis Metadata Backend
```bash
cmake -DWITH_REDIS=ON ..
make -j$(nproc)
```

### Build and Run Unit Tests
```bash
cmake -DBUILD_TESTS=ON ..
make -j$(nproc)
ctest --output-on-failure
```

---

## Running the Grid

### Quick Local Start
You can launch the entire grid locally using the provided automation scripts:

```bash
# Start all daemons in background
./scripts/start_services.sh

# Check running daemon status
./scripts/status.sh

# Stop all services
./scripts/stop_services.sh
```

### Manual Service Startup

Open multiple terminals:

```bash
# Terminal 1: Start Head Server
./build/dfg head-server

# Terminal 2: Start Storage Nodes
./build/dfg cluster-server --server-id 1 --port 8080
./build/dfg cluster-server --server-id 2 --port 8081
./build/dfg cluster-server --server-id 3 --port 8082

# Terminal 3: Start Health Checker
./build/health_checker
```

---

## Client Applications

The grid provides two client interfaces for interacting with the cluster:

### 1. Unified Client CLI (`dfg`)

The primary client interface supporting complete file lifecycle management and diagnostic status:

```bash
# Upload a file (splits, calculates hashes, replicates to storage nodes)
./build/dfg upload <local-file-path> <remote-grid-name>
# Example: ./build/dfg upload document.pdf my_doc.pdf

# Download a file (queries replicas, consensus hash voting, read repair, assembly)
./build/dfg download <remote-grid-name> <output-file-path>
# Example: ./build/dfg download my_doc.pdf /tmp/restored_doc.pdf

# List all stored files with metadata
./build/dfg list

# Check chunk distribution and replica health for a file
./build/dfg status <remote-grid-name>

# Run an end-to-end upload/download verification test
./build/dfg test

# Cluster Server Management (via Head Server Control API)
./build/dfg list-servers
./build/dfg add-server --host 192.168.1.50 --port 8083
./build/dfg remove-server --id 4
```

### 2. Standalone Download Client (`dfg_client`)

A lightweight, dedicated network client binary that communicates directly with the Head Server TCP streaming port (`:9669`):

```bash
# Usage: dfg_client <head_ip> <head_port> <filename> <output_path>
./build/dfg_client 127.0.0.1 9669 my_doc.pdf /tmp/downloaded_doc.pdf
```

#### Client Protocol & Integrity Guarantees

The client and head server communicate over a deterministic line-delimited and binary stream protocol:

1. **Download Request**: Client initiates connection to TCP `:9669` and issues `DOWNLOAD <filename>\n`.
2. **File Hash Handshake**: Server responds with `FILE_HASH <sha256_digest>\n`.
3. **Chunk Streaming**: Server streams each chunk with header `CHUNK <order_id> <chunk_size> <chunk_sha256>\n` followed by raw binary payload.
4. **Per-Chunk Verification**: The client calculates SHA-256 in real-time as each chunk arrives. If corrupted, it aborts and deletes partial data.
5. **End-of-Stream & File Hash Check**: Upon receiving `EOF\n`, the client verifies the full aggregated SHA-256 digest against the expected server hash.

---

## Configuration

Configuration files are located in `config/` in JSON format:

| Config File | Target Component | Key Properties |
|---|---|---|
| [`head_server.json`](config/head_server.json) | Head Server | `server.port`, `storage.chunk_size`, `storage.replication_factor`, `redis.host`, `redis.port` |
| [`cluster_server.json`](config/cluster_server.json) | Cluster Storage | `server.host`, `server.port`, `heartbeat.target_host`, `heartbeat.target_port` |
| [`health_checker.json`](config/health_checker.json) | Health Monitor | `monitoring.heartbeat_timeout`, `monitoring.max_missed_heartbeats`, `failover.enable_auto_failover` |
| [`zookeeper.json`](config/zookeeper.json) | ZK Coordination | `zookeeper.hosts`, `monitor.interval_seconds` |

Environment variables prefixed with `DFG_` override configuration keys (e.g. `DFG_SERVER_PORT=9669`).

---

## Docker Deployment

To launch the distributed grid with Prometheus, Grafana, Loki, and Promtail:

```bash
cd deploy/docker
docker-compose up -d
```

- **Health Checker UI**: `http://localhost:9098`
- **Prometheus Metrics**: `http://localhost:9090`
- **Grafana Dashboards**: `http://localhost:3000` (Login: `admin` / `admin`)

---

## License

This project is licensed under the [GNU General Public License v2.0](LICENSE).

