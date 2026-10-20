# Distributed File Grid

A distributed file storage project in **C++20**. Files are chunked into 64 MB blocks, replicated across storage nodes, and reassembled with SHA-256 integrity verification. See the implementation limits below before relying on its coordination or failover behavior.

---

## What It Does

- **Chunking & Replication**: Splits files into configurable chunks (default: 64 MB), replicates each chunk across $N$ storage nodes (default: 3), and computes SHA-256 digests for end-to-end data integrity.
- **Replica Verification**: Downloads compare chunk hashes across available replicas, select a matching replica, and verify both chunk and full-file checksums. Detected corruption fails the download; automatic repair during reads is not currently implemented.
- **Dynamic Cluster Membership**: Cluster storage servers self-register dynamically with the Head Server's Control API (`:9670`).
- **Heartbeat & System Monitoring**: Storage nodes emit real-time system metrics (CPU, RAM, Disk, Network) to the Health Checker.
- **Coordination Experiments**: The ZooKeeper-shaped adapter is an in-process simulation. It does not connect to an external ensemble or provide coordination between processes.
- **Observability**: Exposes native Prometheus metrics across all nodes and provides pre-configured Grafana dashboards with Loki log aggregation.

---

## Architecture

```
                       ┌────────────────────────────────────────────────────────┐
                       │                   User Applications                    │
                       │   ┌──────────────────┐          ┌──────────────────┐   │
                       │   │  client CLI      │          │ App Integrations │   │
                       │   └─────────┬────────┘          └────────┬─────────┘   │
                       └─────────────┼────────────────────────────┼─────────────┘
                                     │                            │
                                     │ TCP Streaming (:9669)      │
                                     │ (UPLOAD / DOWNLOAD / LIST) │
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
| `dfg` | Server binary — launches and manages backend services (`head-server`, `cluster-server`, `health-checker`, `zk-monitor`, and runtime cluster node management). |
| `client` | Client binary — dedicated CLI for client-side operations (`upload`, `download`, `list`, `test`). Symlinked to `dfg_client` for backward compatibility. |
| `head_server` | Standalone metadata and coordination master. |
| `cluster_server` | Standalone storage node daemon for receiving and serving chunk data. |
| `health_checker` | Standalone health monitoring daemon with web dashboard and Prometheus metrics. |
| `zk_head_server_monitor` | ZooKeeper-based leader election and head server failover monitor. |

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
    zlib1g-dev libfmt-dev redis-server redis-tools libgtest-dev python3
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
# From the repository root:
cmake -S . -B build -DBUILD_TESTS=ON -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel 2
ctest --test-dir build -L unit --output-on-failure --no-tests=error
ctest --test-dir build -L integration --output-on-failure --no-tests=error
```

Tests cover SHA-256 reference vectors and streaming boundaries, thread-pool
concurrency and exception handling, complete heartbeat frames, metadata replica
replacement, server membership, and health recovery. The integration suite starts
a real head server and three storage nodes, verifies empty/binary/2 MB uploads and
downloads, missing-file errors, and downloads with one replica offline. It uses
temporary metadata and separate storage directories, dynamically allocates data
and control ports, and stops only its own processes. Service logs are saved under
`build/integration-logs/`. The services still use their standard metrics and UDP
heartbeat ports, so run the integration suite without another local grid running.
The live tests cover both standalone services and the unified `dfg` binary,
including commands split across TCP writes and upload headers coalesced with payloads.

### Source Layout and Coding Style

Each component keeps public headers in `include/dfg/<component>/` and private
implementation files in `src/`. Executable entry points live in `src/main.cpp`.

| Directory | Responsibility |
|---|---|
| `src/common` | Networking, configuration, hashing, health tracking, and the in-process ZooKeeper adapter |
| `src/metadata` | Shared metadata API with on-disk and optional Redis backends |
| `src/head_server` | Request dispatch, head service startup, upload placement, downloads, and replica repair |
| `src/cluster_server` | Storage service, chunk transfer, and metrics |
| `src/health_checker` | Heartbeat monitoring and health dashboard |
| `src/zk_monitor` | Coordination monitor |
| `src/client` | Client library, diagnostic command, and CLI |
| `src/cli` | Unified server CLI |
| `src/proto/v1` | Protocol schemas; generated C++ stays in the build directory |

Service implementations compile into libraries shared by the standalone binaries
and `dfg`. Declare cross-component interfaces in public headers and link their
CMake targets; keep private headers local to the component. Link `dfg_metadata`
when using `<dfg/metadata/store.hpp>` so Redis configuration propagates correctly.

C++ uses the root `.clang-format` (LLVM style, two spaces, 80 columns).
Use clang-format **21.1.4**, matching CI. Set `CLANG_FORMAT` when the executable
is outside `PATH`.

```bash
make format        # Format owned C++ sources
make format-check  # Check without editing
make test          # Build Release and run isolated unit and integration suites
```

`make clean` removes build artifacts. Runtime storage is managed separately.

### PR Merge Pipeline

`.github/workflows/pr-merge.yml` runs on pull requests targeting `main`, merge queue
checks, pushes to `main`, and manual dispatch. Both Release and Debug builds compile
all binaries and run the unit and live cluster integration suites. Debug enables
AddressSanitizer and UndefinedBehaviorSanitizer, and any sanitizer finding fails
the job. JUnit reports and service logs are uploaded for 14 days. This pipeline
tests the on-disk metadata backend (`WITH_REDIS=OFF`); Redis and the Docker Compose
topology are not covered by this workflow.

To enforce these checks before merging, configure a GitHub branch rule/ruleset for
`main` to require pull requests and the **Merge ready** status check, with branches
up to date (or a merge queue). The workflow supplies this check; branch protection
must be enabled in the repository settings. It does not automatically merge PRs.

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

### Manual Service Startup (`dfg`)

Open multiple terminals:

```bash
# Terminal 1: Start Head Server
./build/dfg head-server

# Terminal 2: Start Storage Nodes
./build/dfg cluster-server --server-id 1 --port 8080
./build/dfg cluster-server --server-id 2 --port 8081
./build/dfg cluster-server --server-id 3 --port 8082

# Terminal 3: Start Health Checker
./build/dfg health-checker

# Terminal 4: Start ZooKeeper Monitor (Optional)
./build/dfg zk-monitor

# Runtime Cluster Node Management (via Head Server Control API :9670)
./build/dfg list-servers
./build/dfg add-server --host 192.168.1.50 --port 8083
./build/dfg remove-server --id 4
```

---

## Client Application (`client`)

The dedicated client CLI (`client`, symlinked as `dfg_client`) handles all client-side file lifecycle operations over TCP streaming (`:9669`) with cryptographic SHA-256 validation:

### 1. Subcommands:

```bash
# Upload a file (calculates SHA-256, streams to head server, replicates to nodes)
./build/client upload <local-file-path> [remote-grid-name]
# Example: ./build/client upload document.pdf my_doc.pdf

# Download a file (replica hash voting, checksum verification, assembly)
./build/client download <remote-grid-name> [output-file-path]
# Example: ./build/client download my_doc.pdf /tmp/restored_doc.pdf

# List all stored files with metadata
./build/client list

# Run end-to-end upload/download verification test
./build/client test
```

### 2. Flags & Compatibility Options:

```bash
# Flag-based upload:
./build/client --server_ip 127.0.0.1 --server_port 9669 --upload /path/to/local_file.iso

# Flag-based download:
./build/client --server_ip 127.0.0.1 --server_port 9669 --download my_doc.pdf -o /tmp/downloaded_doc.pdf

# Legacy positional arguments:
# Usage: client <head_ip> <head_port> <filename> [output_path]
./build/client 127.0.0.1 9669 my_doc.pdf /tmp/downloaded_doc.pdf
```

#### Client Protocol & Integrity Guarantees

The client and head server communicate over a deterministic line-delimited and binary stream protocol:

1. **Upload Protocol**:
   - Client connects to TCP `:9669` and computes the local full-file SHA-256 hash and total chunk count.
   - Sends upload header: `UPLOAD <filename> <filesize> <num_chunks> <sha256_hash>\n`.
   - Streams file chunks: `CHUNK <order_id> <chunk_size> <chunk_sha256>\n` followed by raw chunk bytes.
   - Sends `EOF\n` upon transferring all chunks.
   - Head Server verifies chunk checksums on receipt, stores chunk replicas across cluster nodes concurrently, commits metadata to the configured backend, and returns `SUCCESS\n`.
2. **Download Protocol**:
   - Client sends `DOWNLOAD <filename>\n`.
   - Server queries metadata and returns `FILE_HASH <sha256_digest>\n` and `CHUNKS <num_chunks>\n`.
   - Server compares replica hashes and returns `CHUNK_LOC <order_id> <ip> <public_port> <chunk_sha256>\n` entries followed by the file hash and `EOF\n`.
   - Client fetches chunks in parallel from the storage nodes using `GET_CHUNK <order_id> <filename>\n`; each node returns `CHUNK_SIZE <bytes>\n` followed by the binary payload.
   - **Per-Chunk Verification**: Client verifies each chunk's SHA-256 checksum.
   - **Full-File Verification**: After assembly, the client verifies the reconstructed file's SHA-256 digest against the server digest.

---

## High-Availability & Self-Healing

### Coordination and Failover Limits

Storage nodes report heartbeats, and the head server contains failure-detection
and re-replication routines. The current `ZooKeeperClient` stores nodes in memory
inside each process. Configuring `zookeeper.hosts` does not turn it into a real
ZooKeeper client. Cross-process leader election and coordinated multi-head
failover are therefore not provided by this implementation.

The test suite verifies file operations with a single head server, storage
restarts, replica loss, malformed requests, and failed writes. It does not prove
production high availability. Services currently assume a trusted network;
authentication and transport encryption are not implemented.

### File Operation Rules

- Uploads create a new remote name. Delete an existing file before uploading a
  replacement. Requests for the same name are serialized within the head process.
- Remote names contain at most 200 bytes, with no whitespace, control characters,
  path separators, or `|`. They are names rather than filesystem paths.
- Uploads must supply contiguous chunk IDs starting at zero, the declared total
  size, valid SHA-256 digests, and an `EOF` terminator. Each chunk is limited to
  256 MiB; the standard client uses 64 MiB chunks.
- Downloads fetch at most four chunks at a time. They write a temporary file
  beside the destination and replace it only after complete integrity checks and
  a successful write. A failed download preserves the previous destination.
- On-disk metadata is replaced atomically. If persistence fails, the in-memory
  update is rolled back and the operation reports an error. Storage nodes rebuild
  their chunk index from saved files after restart.
- Failed or interrupted uploads can leave unreferenced chunk files. Automatic
  garbage collection and a transaction spanning all storage nodes are not
  implemented.

### Redis Metadata Backend Improvements
- **Connection Pooling**: `sw::redis::Redis` connection pool is reused across calls, avoiding socket churn.
- **Multi-Replica Chunk Keys**: Replicas use unique keys (`chunk:<id>:<server>`), preventing replica overwrites.
- **Non-blocking Iteration**: Key scans use cursor-based `redis.scan` instead of blocking `redis.keys`.
- **Robust Discovery**: Automatic detection of `redis++` and `hiredis` in `CMakeLists.txt` via `find_package` and include/library fallbacks.

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

## Docker Deployment & E2E Environment Emulation

### Launching the Full Containerized Stack
To launch the complete distributed grid topology (Head Servers, 3 Cluster Nodes, Health Checker, ZooKeeper Monitor, Redis, ZooKeeper, Prometheus, Grafana, Loki, and Promtail):

```bash
cd deploy/docker
docker compose up -d
```

- **Head Server API**: `http://localhost:9670/api/v1/status`
- **Health Checker UI**: `http://localhost:9098`
- **Prometheus Metrics**: `http://localhost:9090`
- **Grafana Dashboards**: `http://localhost:3000` (Login: `admin` / `admin`)

### Containerized E2E System Emulation (Work in Progress 🚧)

The project includes an end-to-end integration test runner [`e2e/e2e_test.sh`](e2e/e2e_test.sh) supporting both local background services and full Docker container emulation:

- **Local E2E Tests**:
  ```bash
  ./e2e/e2e_test.sh
  ```
  Runs 8 end-to-end verification tests covering small and multi-MB file upload/download, SHA-256 validation, client streaming, HTTP Control API, Prometheus metrics, and downloads after replica loss.

- **Full Containerized Grid Emulation (`--system-test`)** *(Work in Progress)*:
  ```bash
  ./e2e/e2e_test.sh --system-test
  ```
  > [!NOTE]
  > **WIP Notice**: Complete cluster emulation under Docker builds the multi-stage images, orchestrates container initialization across all 14 services, and executes client operations against live containers. Automated container health timing, multi-stage build caching, and network bridging optimizations are currently under active development.

---

## License

This project is licensed under the [GNU General Public License v2.0](LICENSE).
