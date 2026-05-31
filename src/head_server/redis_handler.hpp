#ifndef REDIS_HANDLER_HPP
#define REDIS_HANDLER_HPP

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <unordered_map>
#include <vector>

#ifdef WITH_REDIS
#include <sw/redis++/redis++.h>
using namespace sw::redis;
#endif

// Utility: simple ID when file_name isn’t supplied (time-based hex).
inline std::string gen_file_id() {
  auto now =
      std::chrono::high_resolution_clock::now().time_since_epoch().count();
  std::ostringstream os;
  os << std::hex << now;
  return os.str();
}

inline std::string file_key(const std::string &id) { return "file:" + id; }

// Encode/decode "server|path" without JSON.
inline std::string encode_loc(const std::string &server,
                              const std::string &path) {
  // If server/path may contain '|', choose a different delimiter or escape it.
  return server + "|" + path;
}
inline std::pair<std::string, std::string> decode_loc(const std::string &v) {
  auto p = v.find('|');
  if (p == std::string::npos)
    return {v, ""};
  return {v.substr(0, p), v.substr(p + 1)};
}

#ifndef WITH_REDIS
namespace metadata_store {
struct ChunkRecord {
  long long chunk_id{};
  std::string server;
  std::string path;
};

using MetadataMap = std::unordered_map<std::string, std::vector<ChunkRecord>>;

inline std::string db_path() {
  const char *custom = std::getenv("DFG_METADATA_DB");
  return (custom && *custom) ? std::string(custom) : std::string{"/tmp/dfg_metadata.db"};
}

inline MetadataMap load_from_disk() {
  MetadataMap map;
  std::ifstream in(db_path());
  if (!in)
    return map;

  std::string line;
  while (std::getline(in, line)) {
    if (line.empty())
      continue;
    std::istringstream iss(line);
    std::string file_name;
    ChunkRecord record;
    if (!(iss >> file_name >> record.chunk_id >> record.server >> record.path))
      continue;
    map[file_name].push_back(record);
  }
  return map;
}

inline MetadataMap &store() {
  static MetadataMap data = load_from_disk();
  return data;
}

inline std::mutex &store_mutex() {
  static std::mutex m;
  return m;
}

inline void persist_locked() {
  std::ofstream out(db_path(), std::ios::trunc);
  if (!out) {
    std::cerr << "Failed to persist metadata store" << std::endl;
    return;
  }
  for (const auto &kv : store()) {
    for (const auto &chunk : kv.second) {
      out << kv.first << ' ' << chunk.chunk_id << ' ' << chunk.server << ' '
          << chunk.path << '\n';
    }
  }
}

inline void replace_file_chunks(const std::string &file,
                                std::vector<ChunkRecord> &&chunks) {
  std::lock_guard<std::mutex> lock(store_mutex());
  store()[file] = std::move(chunks);
  persist_locked();
}

inline std::vector<ChunkRecord> get_chunks(const std::string &file) {
  std::lock_guard<std::mutex> lock(store_mutex());
  auto it = store().find(file);
  if (it == store().end())
    return {};
  return it->second;
}

inline void remove_file(const std::string &file) {
  std::lock_guard<std::mutex> lock(store_mutex());
  store().erase(file);
  persist_locked();
}

inline std::vector<std::string> list_files_snapshot() {
  std::lock_guard<std::mutex> lock(store_mutex());
  std::vector<std::string> files;
  files.reserve(store().size());
  for (const auto &kv : store())
    files.push_back(kv.first);
  std::sort(files.begin(), files.end());
  return files;
}
} // namespace metadata_store
#endif

#ifdef WITH_REDIS
inline std::string get_redis_connection_string() {
  const char* host = std::getenv("REDIS_HOST");
  const char* port = std::getenv("REDIS_PORT");
  std::string host_str = host ? host : "127.0.0.1";
  std::string port_str = port ? port : "6379";
  return "tcp://" + host_str + ":" + port_str;
}

inline void create_entry(const std::string& request) {
  try {
    Redis redis(get_redis_connection_string()); // primary for writes [1]

    std::istringstream in(request);
    std::string file_name;
    std::getline(in, file_name);

    if (file_name.empty())
      file_name = gen_file_id();
    const std::string key = file_key(file_name);

    // Optional TTL line: "TTL=seconds"
    long long ttl = 0;
    std::streampos after_first_line = in.tellg();
    std::string maybe_ttl;
    if (std::getline(in, maybe_ttl)) {
      if (maybe_ttl.rfind("TTL=", 0) == 0) {
        ttl = std::stoll(maybe_ttl.substr(4));
      } else {
        // Not TTL; rewind to start parsing chunks from this line
        in.clear();
        in.seekg(after_first_line);
      }
    }

    // Batch fields for HSET key field value [5][1]
    std::vector<std::pair<std::string, std::string>> fields;
    std::string line;
    while (std::getline(in, line)) {
      if (line.empty())
        continue;
      std::istringstream ls(line);
      long long chunk_id;
      std::string server, path;
      if (!(ls >> chunk_id >> server >> path)) {
        continue; // skip malformed
      }
      std::string field = "chunk:" + std::to_string(chunk_id);
      fields.emplace_back(field, encode_loc(server, path));
    }

    if (!fields.empty()) {
      redis.hset(key, fields.begin(), fields.end()); // bulk HSET [1][5]
    } else {
      // Optionally create a marker so the hash exists:
      // redis.hset(key, "meta", "created");
    }

    if (ttl > 0) {
      redis.expire(key, std::chrono::seconds{ttl}); // set TTL on hash key [1]
    }

    std::cout << "Created file entry: " << file_name << "\n";
  } catch (const std::exception &e) {
    std::cerr << "create_entry error: " << e.what() << "\n";
  }
}
#else
inline void create_entry(const std::string &request) {
  std::istringstream in(request);
  std::string file_name;
  std::getline(in, file_name);
  if (file_name.empty())
    file_name = gen_file_id();

  std::vector<std::string> raw_lines;
  std::string line;
  while (std::getline(in, line)) {
    if (!line.empty())
      raw_lines.push_back(line);
  }

  if (!raw_lines.empty() && raw_lines.front().rfind("TTL=", 0) == 0) {
    raw_lines.erase(raw_lines.begin());
  }

  std::vector<metadata_store::ChunkRecord> chunks;
  for (const auto &entry_line : raw_lines) {
    std::istringstream ls(entry_line);
    metadata_store::ChunkRecord record;
    if (!(ls >> record.chunk_id >> record.server >> record.path))
      continue;
    chunks.push_back(record);
  }

  metadata_store::replace_file_chunks(file_name, std::move(chunks));
  std::cout << "Created file entry: " << file_name << "\n";
}
#endif

#ifdef WITH_REDIS
inline void read_entry(const std::string& request) {
  try {
    std::istringstream in(request);
    std::string file_name;
    in >> file_name;
    if (file_name.empty()) {
      std::cerr << "read_entry: file_name required\n";
      return;
    }
    const std::string key = file_key(file_name);

    // If reading from a replica, point this connection to the replica host.
    // [20]
    Redis redis(get_redis_connection_string()); // [1]

    if (in.good()) {
      // Specific chunk
      long long chunk_id;
      if (in >> chunk_id) {
        std::string field = "chunk:" + std::to_string(chunk_id);
        auto v = redis.hget(key, field); // optional<string> [1][15]
        if (v) {
          auto [server, path] = decode_loc(*v);
          std::cout << field << " server=" << server << " path=" << path
                    << "\n";
        } else {
          std::cout << "Chunk not found\n";
        }
        return;
      }
    }

    // All chunks
    std::unordered_map<std::string, std::string> all;
    redis.hgetall(key, std::inserter(all, all.end())); // [1][8]
    if (all.empty()) {
      std::cout << "No chunks or file not found\n";
      return;
    }
    for (const auto &kv : all) {
      if (kv.first.rfind("chunk:", 0) == 0) {
        auto [server, path] = decode_loc(kv.second);
        std::cout << kv.first << " server=" << server << " path=" << path
                  << "\n";
      }
    }
  } catch (const std::exception &e) {
    std::cerr << "read_entry error: " << e.what() << "\n";
  }
}
#else
inline void read_entry(const std::string &request) {
  std::istringstream in(request);
  std::string file_name;
  in >> file_name;
  if (file_name.empty()) {
    std::cerr << "read_entry: file_name required" << std::endl;
    return;
  }

  auto chunks = metadata_store::get_chunks(file_name);
  if (chunks.empty()) {
    std::cout << "No chunks or file not found" << std::endl;
    return;
  }

  long long desired_chunk = -1;
  if (in >> desired_chunk) {
    auto it = std::find_if(chunks.begin(), chunks.end(),
                           [desired_chunk](const metadata_store::ChunkRecord &record) {
                             return record.chunk_id == desired_chunk;
                           });
    if (it != chunks.end()) {
      std::cout << "chunk:" << it->chunk_id << " server=" << it->server
                << " path=" << it->path << std::endl;
    } else {
      std::cout << "Chunk not found" << std::endl;
    }
    return;
  }

  for (const auto &record : chunks) {
    std::cout << "chunk:" << record.chunk_id << " server=" << record.server
              << " path=" << record.path << std::endl;
  }
}
#endif

#ifdef WITH_REDIS
inline void delete_entry(const std::string& file_name) {
  try {
    // If file_name looks like "name#chunk:3", delete that field; else delete
    // hash. [15]
    std::string base = file_name;
    std::string field;

    auto pos = file_name.find("#chunk:");
    if (pos != std::string::npos) {
      base = file_name.substr(0, pos);
      field = file_name.substr(pos + 1); // "chunk:3"
    }

    const std::string key = file_key(base);
    Redis redis("tcp://127.0.0.1:6379"); // [1]

    if (!field.empty()) {
      long long n = redis.hdel(key, field); // [1][15]
      std::cout << "Removed fields: " << n << "\n";
    } else {
      long long n = redis.del(key); // [1]
      std::cout << "Removed keys: " << n << "\n";
    }
  } catch (const std::exception &e) {
    std::cerr << "delete_entry error: " << e.what() << "\n";
  }
}
#else
inline void delete_entry(const std::string& file_name) {
  if (file_name.empty())
    return;
  metadata_store::remove_file(file_name);
  std::cout << "Removed keys: 1" << std::endl;
}
#endif

#ifdef WITH_REDIS
// Configure the local Redis instance to replicate from the given "host:port".
// Issues REPLICAOF via the generic command interface; returns 0 on success.
// [19]
inline int create_replication(const std::string& ip_address) {
  std::cout << "Creating replication server\n";
  std::cout
      << "Current machine Slave , master server in the sentient protocol is "
      << ip_address << std::endl;
  try {
    auto pos = ip_address.find(':');
    std::string host = ip_address.substr(0, pos);
    int port = (pos == std::string::npos)
                   ? 6379
                   : std::stoi(ip_address.substr(pos + 1));

    Redis redis(get_redis_connection_string()); // local node to become a replica [1]
    redis.command("REPLICAOF", host,
                  std::to_string(port)); // server-side replication [19]
    return 0;
  } catch (const std::exception &e) {
    std::cerr << "create_replication error: " << e.what() << "\n";
    return -1;
  }
}
#else
inline int create_replication(const std::string& ip_address) {
  (void)ip_address;
  std::cout << "Redis disabled - replication simulated locally" << std::endl;
  return 0;
}
#endif

#ifdef WITH_REDIS
// Check if Redis is already running by trying to connect
inline bool is_redis_running(const std::string& host = "127.0.0.1", int port = 6379) {
  try {
    // Use the provided host/port for checking, not the default connection string
    Redis redis("tcp://" + host + ":" + std::to_string(port));
    redis.ping();
    return true;
  } catch (const std::exception&) {
    return false;
  }
}

inline int start_server() {
  // Check if Redis is already running
  if (is_redis_running()) {
    std::cout << "Redis server is already running, skipping daemon creation" << std::endl;
    return 0;
  }

  std::cout << "Starting Redis server daemon..." << std::endl;

  // First fork
  pid_t pid = fork();
  if (pid < 0)
    return 1;
  if (pid > 0)
    return 0; // parent exits

  // New session
  if (setsid() < 0)
    _exit(1);

  // Second fork
  pid = fork();
  if (pid < 0)
    _exit(1);
  if (pid > 0)
    _exit(0); // intermediate exits

  // Detach
  // chdir("/");
  umask(027);

  // Detach stdio
  for (int fd = 0; fd < 3; ++fd)
    close(fd);
  open("/dev/null", O_RDONLY);                   // stdin
  open("../../logs/current_logs.txt", O_WRONLY); // stdout
  open("/dev/null", O_WRONLY);                   // stderr

  // IMPORTANT: Start Redis with NO arguments (no config file)
  // This uses Redis built-in defaults. Suitable for testing/dev.
  // For production, prefer a config file.
  execl("/usr/bin/redis-server", "redis-server", (char *)nullptr);

  // If execl fails
  _exit(1);
}
#else
// Check if Redis would be running (always false when disabled)
inline bool is_redis_running(const std::string& host = "127.0.0.1", int port = 6379) {
  (void)host; (void)port; // suppress unused parameter warnings
  return false;
}

inline int start_server() {
  std::cout << "Redis disabled - using in-memory storage simulation" << std::endl;
  return 0; // Return success since we're simulating
}
#endif

#ifdef WITH_REDIS
inline std::vector<std::string> list_all_files() {
  try {
    Redis redis(get_redis_connection_string());
    std::vector<std::string> files;
    std::vector<std::string> keys;
    redis.keys("file:*", std::back_inserter(keys));
    files.reserve(keys.size());
    for (const auto &key : keys) {
      if (key.rfind("file:", 0) == 0) {
        files.push_back(key.substr(5));
      }
    }
    std::sort(files.begin(), files.end());
    return files;
  } catch (const std::exception &e) {
    std::cerr << "list_all_files error: " << e.what() << std::endl;
    return {};
  }
}
#else
inline std::vector<std::string> list_all_files() {
  return metadata_store::list_files_snapshot();
}
#endif

inline void start_daemon() {
  std::cout << "Starting Head Server daemon..." << std::endl;
  
#ifdef WITH_REDIS
  // Check if Redis is already running first
  if (is_redis_running()) {
    std::cout << "Redis server is already running, proceeding with Head Server startup" << std::endl;
  } else {
    // Start Redis server in background
    if (start_server() != 0) {
      std::cerr << "Failed to start Redis server" << std::endl;
      return;
    }
    
    // Give Redis time to start
    std::cout << "Waiting for Redis server to start..." << std::endl;
    sleep(2);
    
    // Verify Redis started successfully
    if (!is_redis_running()) {
      std::cerr << "Redis server failed to start properly" << std::endl;
      return;
    }
    std::cout << "Redis server started successfully" << std::endl;
  }
#else
  // Redis is disabled, just simulate successful startup
  std::cout << "Redis disabled - using in-memory storage simulation" << std::endl;
#endif
  
  std::cout << "Head Server daemon is ready" << std::endl;
}

#endif // REDIS_HANDLER_HPP
