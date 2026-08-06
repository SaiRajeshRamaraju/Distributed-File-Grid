# Distributed File Grid

A distributed file storage system that splits files into chunks, each chunk is replicated across a cluster of servers, and reassembles them on demand.
---
## What It Does

You give it a file. It breaks it into 64 MB chunks, spreads those chunks across multiple storage nodes with configurable replication, and stitches them back together when you ask for the file again.

Behind the scenes:

- A **head server** manages metadata and orchestrates uploads/downloads
- **Cluster servers** store the actual chunks and report their health via heartbeats
- A **health checker** watches over the cluster and flags unhealthy nodes
- An optional **ZooKeeper monitor** handles leader election when running multiple head servers

Everything talks over Protocol Buffers. Metrics are exported to Prometheus. Dashboards are pre-configured for Grafana.

---

## Getting Started

### Prerequisites

| Dependency | Why |
|---|---|
| GCC 13+ or Clang 16+ | C++20 with coroutine support |
| CMake 3.16+ | Build system |
| protobuf + protoc | Serialization (fetched at build time if missing) |
| zlib | Compression |

Abseil and Prometheus-cpp are fetched automatically via CMake's `FetchContent`. You don't need to install them.

### Build

```bash
git clone https://github.com/SaiRajeshRamaraju/Distributed-File-Grid.git
cd Distributed-File-Grid

mkdir build && cd build
cmake ..
make -j$(nproc)
```

This produces five binaries in `build/`:

| Binary | Purpose |
|---|---|
| `dfg` | Unified CLI — the primary way to interact with everything |
| `head_server` | Standalone head server (if you prefer separate processes) |
| `cluster_server` | Standalone cluster server |
| `health_checker` | Heartbeat-based health monitor |
| `zk_head_server_monitor` | ZooKeeper-based leader election monitor |

### Run

Open three terminals:

```bash
# Terminal 1 — start the head server
./build/dfg head-server

# Terminal 2 — start a storage node
./build/dfg cluster-server --server-id 1 --ip 127.0.0.1 --port 8080

# Terminal 3 — upload and download a file
./build/dfg upload photo.jpg
./build/dfg download photo.jpg /tmp/restored.jpg
```

That's it. No containers, no config files, no ceremony.

---

## Usage

All commands go through the `dfg` binary:

```bash
# File operations
dfg upload <localfile-name>        # Upload a file to the grid
dfg download <name> <savingfile-path>      # Download a file from the grid
dfg list                              # List all stored files

# Server management (talks to head server's control API)
dfg add-server --host 10.0.0.5 --port 8081
dfg remove-server --id 3
dfg list-servers

# Start services
dfg head-server                       # Start head server
dfg cluster-server --server-id 2      # Start cluster server

# Diagnostics
dfg test                              # Run a built-in upload/download round-trip
dfg --version
```

### Configuration

Config files live in `config/` and use plain JSON:

| File | Controls |
|---|---|
| `head_server.json` | Ports, replication factor, chunk size, control API |
| `cluster_server.json` | Server ID, bind address, heartbeat target |
| `health_checker.json` | Heartbeat timeout, max missed beats |
| `zookeeper.json` | ZK ensemble hosts, session timeout |

Every setting can be overridden with an environment variable prefixed with `DFG_`. For example, `DFG_SERVER_PORT=9669` overrides `server.port` in the JSON.

### Metadata Storage

- **Default**: An embedded on-disk store at `/tmp/dfg_metadata.db` (override with `DFG_METADATA_DB`)
- **Redis** (optional): Build with `cmake -DWITH_REDIS=ON ..` for production-grade metadata persistence

---

## Architecture

```
                        ┌─────────────────┐
                        │    dfg CLI      │
                        └────────┬────────┘
                                 │
                    ┌────────────▼────────────┐
                    │      Head Server        │
                    │  - metadata management  │
                    │  - upload/download      │
                    │  - control API (:9670)  │
                    │  - heartbeat receiver   │
                    └────┬──────┬──────┬─────┘
                         │      │      │
              ┌──────────▼──┐ ┌─▼────┐ ┌▼──────────┐
              │  Cluster 1  │ │  C2  │ │  Cluster 3 │
              │  :8080      │ │:8081 │ │  :8082     │
              │  chunks/    │ │      │ │  chunks/   │
              └─────────────┘ └──────┘ └────────────┘
                    ▲              ▲             ▲
                    │   heartbeats (UDP)         │
                    └──────────┬────────────────┘
                               │
                    ┌──────────▼──────────┐
                    │   Health Checker     │
                    │   + ZK Monitor       │
                    └─────────────────────┘
```

### How Upload Works

1. The CLI sends the file to the head server
2. The head server splits it into 64 MB chunks
3. Each chunk is replicated to *N* cluster servers (default: 3)
4. Metadata (chunk→server mappings) is stored in the metadata backend
5. The cluster servers send heartbeats back to confirm they're alive

### How Download Works

1. The CLI asks the head server for the file
2. The head server looks up which cluster servers hold each chunk
3. Chunks are fetched in parallel and reassembled
4. The reconstructed file is written to the output path

### Health Monitoring

Cluster servers send UDP heartbeats every second containing system metrics (CPU, RAM, disk, network). The head server tracks these and marks a server as unhealthy after configurable missed beats. The health checker provides an additional monitoring layer with Prometheus-compatible metrics.

---

## Docker Deployment

For a full setup with monitoring, use Docker Compose:

```bash
cd deploy/docker
docker-compose up -d
```

This spins up:

| Service | Port | Description |
|---|---|---|
| Head Server | 9669 | Metadata + file operations |
| Cluster Servers ×3 | 8080–8082 | Chunk storage |
| Health Checker | 9091 | Heartbeat monitor |
| ZK Monitor | — | Leader election |
| Prometheus | 9090 | Metrics collection |
| Grafana | 3000 | Dashboards (admin/admin) |

Pre-built Grafana dashboards are included for cluster overview, per-server metrics, and log aggregation.

---

## Building with Options

```bash
# Default build (no Redis, no tests)
cmake ..

# With Redis metadata backend
cmake -DWITH_REDIS=ON ..

# With tests
cmake -DBUILD_TESTS=ON ..

# Release build
cmake -DCMAKE_BUILD_TYPE=Release ..
```

### Makefile Shortcuts

```bash
make build          # Build everything
make clean          # Remove build artifacts
make test           # Run the built-in test suite
make start          # Start all services via scripts/
make stop           # Stop all services
make install-deps   # Install system deps (Ubuntu/Debian)
```

---

## Troubleshooting

**Build fails with protobuf errors**
Make sure `protoc` is installed and matches the version of `libprotobuf-dev`. On Arch, `pacman -S protobuf` gets you both.

**Services won't start — port in use**
Default ports: head server on 9669, control API on 9670, cluster servers on 8080+, heartbeat on 9000. Check with `ss -tlnp | grep 9669`.

**Uploads fail immediately**
Make sure at least one cluster server is running and has registered with the head server. Check the head server's console output for registration messages.

**Zero metrics in Prometheus/Grafana**
Verify the cluster server's metrics exporter is bound — look for the `Prometheus metrics on 0.0.0.0:9091/metrics` log line at startup.

---

## Tech Stack

| Component | Technology |
|---|---|
| Language | C++20 |
| Serialization | Protocol Buffers v3 |
| Build | CMake 3.16+ |
| Metrics | Prometheus-cpp |
| Logging | Abseil |
| Coordination | Built-in ZooKeeper client (no external C deps) |
| Deployment | Docker, Docker Compose |
| Monitoring | Grafana + Loki + Promtail |

## License

This project is licensed under the [GNU General Public License v2.0](LICENSE).
