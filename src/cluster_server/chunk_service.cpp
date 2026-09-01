#include "metrics_exporter.hpp"
#include "dfg/sha256.hpp"
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <dfg/async_net.hpp>
#include <dfg/system_info.hpp>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <functional>
#include <future>
#include <iostream>
#include <mutex>
#include <queue>
#include <sstream>
#include <string>
#include <sys/uio.h>
#include <thread>
#include <unordered_map>
#include <vector>

#include "file_transfer.pb.h"
#include <arpa/inet.h>
#include <chrono>
#include <dfg/thread_pool.hpp>

// ─────────────────── Awaiter: bridge future → coroutine ───────────────────
// Suspends the coroutine, polls the future on reactor ticks via a timerfd,
// and resumes once the result is ready.
template <typename T>
struct FutureAwaiter {
    std::shared_future<T> fut;
    async_hb::Reactor* reactor;

    bool await_ready() const noexcept {
        return fut.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
    }

    void await_suspend(std::coroutine_handle<> h) {
        auto r = reactor;
        auto shared_h = std::make_shared<std::coroutine_handle<>>(h);
        
        auto poll = [r, f = fut, shared_h](async_hb::Reactor& rx) -> async_hb::task {
            while (f.wait_for(std::chrono::microseconds(0)) != std::future_status::ready) {
                co_await rx.sleep_for(std::chrono::milliseconds(1));
            }
            if (*shared_h && !shared_h->done()) {
                shared_h->resume();
            }
            co_return;
        };

        reactor->spawn(poll(*reactor));
    }

    T await_resume() {
        return fut.get();
    }
};

template <typename T>
FutureAwaiter<T> await_future(async_hb::Reactor& r, std::future<T> f) {
    return FutureAwaiter<T>{f.share(), &r};
}

// Global I/O thread pool (shared by all storage operations)
inline dfg::ThreadPool& io_pool() {
    static dfg::ThreadPool pool(4);
    return pool;
}

#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
namespace fs = std::filesystem;

// ──────────────────────── Chunk Storage ────────────────────────
class ChunkStorage {
private:
  std::string storage_path = "/var/cluster_storage/";
  std::unordered_map<std::string, std::string> chunk_registry;
  std::mutex registry_mutex;

  int ensure_storage_directory() {
    // Resolve script path relative to the executable (build/../scripts/)
    fs::path exe_dir = fs::read_symlink("/proc/self/exe").parent_path();
    fs::path script = (exe_dir / "../scripts/make_directory.sh").lexically_normal();
    std::string cmd = "bash " + script.string() + " 2>&1";
    FILE *pipe = popen(cmd.c_str(), "r");
    if (!pipe) {
      std::cerr << "popen failed to create pipe and run bash script"
                << std::endl;
      return -1;
    }
    std::array<char, 128> buffer;
    std::string output;
    while (fgets(buffer.data(), buffer.size(), pipe) != nullptr) {
      output += buffer.data();
    }
    int returnCode = pclose(pipe);
    int exitStatus = WEXITSTATUS(returnCode);
    if (exitStatus != 0) {
      std::cerr << std::format("BASH script error (EXIT code \"{}\")\n {}",
                               exitStatus, output)
                << std::endl;
      return -1;
    } else {
      std::cout << output << std::endl;
    }
    return 0;
  }

  std::string generate_chunk_path(const std::string &chunk_id) {
    auto now = std::chrono::system_clock::now();
    std::time_t t = std::chrono::system_clock::to_time_t(now);
    std::stringstream ss;
    ss << storage_path << "chunk_" << chunk_id << "_" << t << ".dat";
    return ss.str();
  }

public:
  ChunkStorage() {
    int status = ensure_storage_directory();
    if (status != 0) {
      throw std::runtime_error("Error creating storage directory");
    }
  }

  // ── Async store: offload to I/O thread pool ──
  std::future<bool> async_store_chunk(const std::string &chunk_id,
                                      std::vector<char> data) {
    // Capture by value so the data lives in the pool thread.
    auto path = generate_chunk_path(chunk_id);
    auto *self = this;
    return io_pool().submit([self, chunk_id, path,
                             data = std::move(data)]() -> bool {
      int fd =
          ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
      if (fd < 0) {
        std::cerr << "Failed to create chunk file: " << path << std::endl;
        return false;
      }

      size_t written = 0;
      while (written < data.size()) {
        ssize_t n = ::write(fd, data.data() + written, data.size() - written);
        if (n < 0) {
          if (errno == EINTR)
            continue;
          std::cerr << "Write error for chunk " << chunk_id << ": "
                    << strerror(errno) << std::endl;
          ::close(fd);
          return false;
        }
        written += static_cast<size_t>(n);
      }
      ::fdatasync(fd);
      ::close(fd);

      {
        std::lock_guard<std::mutex> lock(self->registry_mutex);
        self->chunk_registry[chunk_id] = path;
      }

      std::cout << "Async stored chunk " << chunk_id << " (" << data.size()
                << " bytes)" << std::endl;
      return true;
    });
  }

  // ── Async retrieve: offload to I/O thread pool ──
  std::future<std::vector<char>>
  async_retrieve_chunk(const std::string &chunk_id) {
    std::string chunk_path;
    {
      std::lock_guard<std::mutex> lock(registry_mutex);
      auto it = chunk_registry.find(chunk_id);
      if (it == chunk_registry.end()) {
        // Return an immediately-ready future with empty data.
        std::promise<std::vector<char>> p;
        p.set_value({});
        return p.get_future();
      }
      chunk_path = it->second;
    }

    return io_pool().submit([chunk_id, chunk_path]() -> std::vector<char> {
      int fd = ::open(chunk_path.c_str(), O_RDONLY | O_CLOEXEC);
      if (fd < 0) {
        std::cerr << "Failed to open chunk file: " << chunk_path << std::endl;
        return {};
      }

      off_t file_size = ::lseek(fd, 0, SEEK_END);
      ::lseek(fd, 0, SEEK_SET);

      std::vector<char> data(static_cast<size_t>(file_size));
      size_t total_read = 0;
      while (total_read < data.size()) {
        ssize_t n =
            ::read(fd, data.data() + total_read, data.size() - total_read);
        if (n < 0) {
          if (errno == EINTR)
            continue;
          ::close(fd);
          return {};
        }
        if (n == 0)
          break;
        total_read += static_cast<size_t>(n);
      }
      ::close(fd);

      std::cout << "Async retrieved chunk " << chunk_id << " (" << data.size()
                << " bytes)" << std::endl;
      return data;
    });
  }

  bool delete_chunk(const std::string &chunk_id) {
    try {
      std::string chunk_path;
      {
        std::lock_guard<std::mutex> lock(registry_mutex);
        auto it = chunk_registry.find(chunk_id);
        if (it == chunk_registry.end()) {
          std::cerr << "Chunk id " << chunk_id
                    << "not found in chunk registry. Error deleting chunk"
                    << std::endl;
          return false;
        }
        chunk_path = it->second;
        chunk_registry.erase(it);
      }

      fs::remove(chunk_path);
      std::cout << "Deleted chunk " << chunk_id << std::endl;
      return true;
    } catch (const std::exception &e) {
      std::cerr << "Delete chunk from local registyr. But error deleting chunk "
                   "in the filesystem "
                << chunk_id << ": " << e.what() << std::endl;
      return false;
    }
  }

  std::vector<std::string> list_chunks() {
    std::vector<std::string> chunks;
    {
      std::lock_guard<std::mutex> lock(registry_mutex);
      for (const auto &[chunk_id, path] : chunk_registry) {
        chunks.push_back(chunk_id);
      }
    }
    return chunks;
  }

  size_t get_storage_usage() {
    size_t total_size = 0;
    try {
      for (const auto &entry : fs::recursive_directory_iterator(storage_path)) {
        if (entry.is_regular_file()) {
          total_size += entry.file_size();
        }
      }
    } catch (const std::exception &e) {
      std::cerr << "Error calculating storage usage: " << e.what() << std::endl;
    }
    return total_size;
  }
};

class ClusterServerService {
private:
  ChunkStorage storage;
  MetricsExporter exporter;
  int server_id;
  std::string server_ip;
  int port;
  bool running = false;

  // Coroutine that periodically samples system metrics (CPU, RAM, disk,
  // network). This calls sleep(1) internally, but it's running in the reactor
  // so we use reactor.sleep_for to avoid blocking the event loop, then do a
  // quick blocking sample.
  async_hb::task metrics_sampler(async_hb::Reactor &reactor) {
    auto &monitor = CachedSystemMonitor::instance();
    while (running) {
      // Take an initial sample immediately (blocks ~1s)
      monitor.sample();
      // Wait before next sample (total period ≈ interval + 1s)
      co_await reactor.sleep_for(std::chrono::seconds(4));
    }
  }

  async_hb::task heartbeat_sender(async_hb::Reactor &reactor) {
    // Resolve health checker address from env or default
    const char *hc_host_env = std::getenv("HEALTH_CHECKER_HOST");
    std::string hc_host = hc_host_env ? hc_host_env : "127.0.0.1";
    int hc_port = 9000;

    sockaddr_in hc_addr{};
    if (!async_hb::resolve_ipv4(hc_host, static_cast<uint16_t>(hc_port),
                                hc_addr)) {
      std::cerr << "Could not resolve health checker at " << hc_host
                << std::endl;
      co_return;
    }

    int sfd = ::socket(AF_INET, SOCK_DGRAM | SOCK_CLOEXEC, 0);
    if (sfd < 0) {
      std::cerr << "Heartbeat UDP socket creation failed\n";
      co_return;
    }
    if (async_hb::set_nonblock(sfd) < 0) {
      std::cerr << "Heartbeat socket set_nonblock failed\n";
      ::close(sfd);
      co_return;
    }
    // Connect the UDP socket so we can use send() instead of sendto()
    if (::connect(sfd, reinterpret_cast<sockaddr *>(&hc_addr),
                  sizeof(hc_addr)) < 0) {
      std::cerr << "Heartbeat UDP connect failed\n";
      ::close(sfd);
      co_return;
    }

    std::string local_ip_port = server_ip + ":" + std::to_string(port);

    auto &monitor = CachedSystemMonitor::instance();

    while (running) {
      try {
        heart_beat::v1::HeartBeat hb;
        hb.set_server_id(server_id);
        hb.set_ip(local_ip_port);
        *hb.mutable_timestamp() =
            google::protobuf::util::TimeUtil::GetCurrentTime();

        // Populate system metrics from the cached monitor
        if (monitor.has_sample()) {
          auto usage = monitor.get();
          hb.set_cpu_usage(usage.cpu_usage);
          hb.set_total_storage_used(usage.disk_usage);
          hb.set_ram_usage(usage.ram_usage);
          hb.set_disk_usage(usage.disk_usage);
          hb.set_network_in(usage.network_in);
          hb.set_network_out(usage.network_out);
          hb.set_network_in_bytes_per_sec(usage.network_in_bytes_per_sec);
          hb.set_network_out_bytes_per_sec(usage.network_out_bytes_per_sec);
        } else {
          // No sample yet — send one-shot values (blocks briefly)
          auto [diskGB, diskPct] = getDiskUsageGBPercent();
          auto [ramGB, ramPct] = getRamUsageGBPercent();
          hb.set_total_storage_used(diskPct);
          hb.set_disk_usage(diskPct);
          hb.set_ram_usage(ramPct);
        }

        auto frame = async_hb::build_frame(hb);
        co_await async_hb::async_send_all(reactor, sfd, frame.data(),
                                          frame.size());
      } catch (const std::exception &e) {
        std::cerr << "Heartbeat send error: " << e.what() << std::endl;
      }
      co_await reactor.sleep_for(std::chrono::seconds(10));
    }
    ::close(sfd);
  }

  std::atomic<int> active_connections{0};

  struct ConnectionTracker {
      std::atomic<int>& counter;
      ConnectionTracker(std::atomic<int>& c) : counter(c) { counter++; }
      ~ConnectionTracker() { counter--; }
  };

  async_hb::task handle_connection(async_hb::Reactor &r, int cfd) {
    ConnectionTracker tracker(active_connections);
    try {
      uint8_t header[8];
      co_await async_hb::async_read_exact(r, cfd, header, 8);

      uint32_t type_net, len_net;
      memcpy(&type_net, header, 4);
      memcpy(&len_net, header + 4, 4);

      uint32_t type = ntohl(type_net);
      uint32_t len = ntohl(len_net);

      std::vector<uint8_t> buf(len);
      if (len > 0) {
        co_await async_hb::async_read_exact(r, cfd, buf.data(), len);
      }

      auto t_start = std::chrono::high_resolution_clock::now();

      if (type == 1) { // ChunkData — async store
        file_transfer::v1::ChunkData req;
        if (!req.ParseFromArray(buf.data(), len))
          throw std::runtime_error("Parse chunk error");

        std::vector<char> data(req.data().begin(), req.data().end());

        // Offload disk write to I/O thread pool, await without blocking reactor
        auto fut = storage.async_store_chunk(req.chunk_id(), std::move(data));
        bool ok = co_await await_future(r, std::move(fut));

        file_transfer::v1::ChunkResponse resp;
        resp.set_success(ok);
        resp.set_chunk_id(req.chunk_id());
        if (!ok)
          resp.set_error_message("Failed to store chunk on disk");

        std::string serialized;
        resp.SerializeToString(&serialized);
        uint32_t resp_len = htonl(serialized.size());

        co_await async_hb::async_send_all(
            r, cfd, reinterpret_cast<const uint8_t *>(&resp_len), 4);
        co_await async_hb::async_send_all(
            r, cfd, reinterpret_cast<const uint8_t *>(serialized.data()),
            serialized.size());

        auto t_end = std::chrono::high_resolution_clock::now();
        exporter.record_message(
            len, std::chrono::duration_cast<std::chrono::nanoseconds>(t_end -
                                                                      t_start)
                     .count());

      } else if (type == 2) { // FetchChunkRequest — async retrieve
        file_transfer::v1::FetchChunkRequest req;
        if (!req.ParseFromArray(buf.data(), len))
          throw std::runtime_error("Parse fetch error");

        // Offload disk read to I/O thread pool
        auto fut = storage.async_retrieve_chunk(req.chunk_id());
        std::vector<char> data = co_await await_future(r, std::move(fut));
        bool ok = !data.empty();

        file_transfer::v1::FetchChunkResponse resp;
        resp.set_success(ok);
        resp.set_chunk_id(req.chunk_id());
        if (ok) {
          resp.set_data(data.data(), data.size());
        } else {
          resp.set_error_message("Chunk not found on this server");
        }

        std::string serialized;
        resp.SerializeToString(&serialized);
        uint32_t resp_len = htonl(serialized.size());

        co_await async_hb::async_send_all(
            r, cfd, reinterpret_cast<const uint8_t *>(&resp_len), 4);
        co_await async_hb::async_send_all(
            r, cfd, reinterpret_cast<const uint8_t *>(serialized.data()),
            serialized.size());
      } else if (type == 3) { // FetchHashRequest — async retrieve for hashing
        file_transfer::v1::FetchHashRequest req;
        if (!req.ParseFromArray(buf.data(), len))
          throw std::runtime_error("Parse fetch hash error");

        // Offload disk read to I/O thread pool
        auto fut = storage.async_retrieve_chunk(req.chunk_id());
        std::vector<char> data = co_await await_future(r, std::move(fut));
        bool ok = !data.empty();

        file_transfer::v1::FetchHashResponse resp;
        resp.set_success(ok);
        resp.set_chunk_id(req.chunk_id());
        if (ok) {
          resp.set_hash(dfg::hash::sha256(data));

          auto usage = system_monitor();
          resp.set_load_score(usage.cpu_usage + usage.ram_usage);
          resp.set_active_connections(active_connections.load());
        } else {
          resp.set_error_message("Chunk not found on this server");
        }

        std::string serialized;
        resp.SerializeToString(&serialized);
        uint32_t resp_len = htonl(serialized.size());

        co_await async_hb::async_send_all(
            r, cfd, reinterpret_cast<const uint8_t *>(&resp_len), 4);
        co_await async_hb::async_send_all(
            r, cfd, reinterpret_cast<const uint8_t *>(serialized.data()),
            serialized.size());
      } else {
        std::cerr << "Unknown message type: " << type << std::endl;
      }
    } catch (const std::exception &e) {
      std::cerr << "handle_connection error: " << e.what() << std::endl;
    }
    ::close(cfd);
    co_return;
  }

  async_hb::task chunk_server(async_hb::Reactor &reactor) {
    int transfer_port = port + 100; // NOTE: Why not a static port id rather
                                    // than 100+ current server port? Come out
                                    // with a static port number
    int lfd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (lfd < 0) {
      std::cerr << "Listen socket failed\n";
      co_return;
    }
    int yes = 1;
    setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(transfer_port);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if (::bind(lfd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
      std::cerr << "Bind failed for chunk server\n";
      ::close(lfd);
      co_return;
    }
    if (::listen(lfd, 64) < 0) { // Increased backlog for higher concurrency
      std::cerr << "Listen failed\n";
      ::close(lfd);
      co_return;
    }
    if (async_hb::set_nonblock(lfd) < 0) {
      std::cerr << "Failed to set nonblocking\n";
      ::close(lfd);
      co_return;
    }

    std::cout << "Chunk transfer server started on port " << transfer_port
              << std::endl;

    auto print_stats = [this](async_hb::Reactor &r) -> async_hb::task {
      while (running) {
        co_await r.sleep_for(std::chrono::seconds(5));
        auto usage = system_monitor();
        exporter.update_system_metrics(usage.cpu_usage, usage.ram_usage,
                                       usage.disk_usage);

        // Print out stats every minute approx
        static int ticks = 0;
        if (ticks++ % 12 == 0) {
          std::cout << "Server " << server_id << " - CPU: " << usage.cpu_usage
                    << "%, RAM: " << usage.ram_usage
                    << "%, Disk: " << usage.disk_usage << "%" << std::endl;
          size_t storage_usage = storage.get_storage_usage();
          std::cout << "Storage usage: " << storage_usage / (1024 * 1024)
                    << " MB" << std::endl;
        }
      }
    };
    reactor.spawn(print_stats(reactor));

    while (running) {
      int cfd = -1;
      while (true) {
        sockaddr_in peer{};
        socklen_t len = sizeof(peer);
        cfd = ::accept4(lfd, reinterpret_cast<sockaddr *>(&peer), &len,
                        SOCK_CLOEXEC | SOCK_NONBLOCK);
        if (cfd >= 0)
          break;
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
          co_await reactor.wait_readable(lfd);
        } else {
          std::cerr << "accept error\n";
          break;
        }
      }
      if (cfd >= 0) {
        reactor.spawn(handle_connection(reactor, cfd));
      }
    }
    ::close(lfd);
  }

public:
  ClusterServerService(int id, const std::string &ip, int p)
      : exporter("0.0.0.0:" + std::to_string(9090 + id)), server_id(id),
        server_ip(ip), port(p) {}

  void start() {
    running = true;
    std::cout << "Starting Cluster Server " << server_id << " on " << server_ip
              << ":" << port << std::endl;
    std::cout << "Metrics exported on port: " << (9090 + server_id)
              << std::endl;
    std::cout << "Async I/O thread pool: 4 threads" << std::endl;

    async_hb::Reactor reactor;

    // Start system metrics sampler (feeds cached data to heartbeat)
    reactor.spawn(metrics_sampler(reactor));

    // Start heartbeat sender
    reactor.spawn(heartbeat_sender(reactor));

    // Start chunk server
    reactor.spawn(chunk_server(reactor));

    // Run the reactor
    reactor.run();
  }
  // We changed the running state to false, so this is changing the state once
  // this function called.
  void stop() {
    running = false;
    std::cout << "Stopping Cluster Server " << server_id << std::endl;
  }

  // Chunk operations API (sync wrappers, still available for external callers)
  bool delete_chunk(const std::string &chunk_id) {
    return storage.delete_chunk(chunk_id);
  }

  std::vector<std::string> list_chunks() { return storage.list_chunks(); }
};

// Global cluster server instance
static std::unique_ptr<ClusterServerService> g_cluster_server;

int start_cluster_server(int server_id, const char *ip, int port) {
  try {
    g_cluster_server =
        std::make_unique<ClusterServerService>(server_id, ip, port);
    g_cluster_server->start();
    return 0;
  } catch (const std::exception &e) {
    std::cerr << "Error starting cluster server: " << e.what() << std::endl;
    return -1;
  }
}

void stop_cluster_server() {
  if (g_cluster_server) {
    g_cluster_server->stop();
    g_cluster_server.reset();
  }
}
