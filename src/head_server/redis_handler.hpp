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
#include <map>
#include <unordered_map>
#include <vector>

#ifdef WITH_REDIS
#include <sw/redis++/redis++.h>
using namespace sw::redis;
#endif

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

namespace metadata_store {
struct ChunkRecord {
  long long chunk_id{};
  std::string server;
  std::string path;
  std::string checksum;
  int replica_count{1};
};

struct FileRecord {
    std::string file_hash;
    int replica_count{0};
    std::vector<ChunkRecord> chunks;
};

#ifndef WITH_REDIS
using MetadataMap = std::unordered_map<std::string, FileRecord>;

inline std::string db_path() {
  const char *custom = std::getenv("DFG_METADATA_DB");
  return (custom && *custom) ? std::string(custom)
                             : std::string{"/tmp/dfg_metadata.db"};
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
    
    // We differentiate file hash line from chunk line by seeing if second word is "HASH=" or "REPLICAS="
    std::string token2;
    if (!(iss >> file_name >> token2))
      continue;
    
    if (token2.rfind("HASH=", 0) == 0) {
        map[file_name].file_hash = token2.substr(5);
    } else if (token2.rfind("REPLICAS=", 0) == 0) {
        try { map[file_name].replica_count = std::stoi(token2.substr(9)); } catch (...) {}
    } else {
        try {
            ChunkRecord record;
            record.chunk_id = std::stoll(token2);
            if (iss >> record.server >> record.path) {
                iss >> record.checksum;
                if (!(iss >> record.replica_count)) {
                    record.replica_count = 1;
                }
                map[file_name].chunks.push_back(record);
            }
        } catch (...) {
            // Ignore malformed line
        }
    }
  }

  // Ensure replica counts are consistent
  for (auto &[name, rec] : map) {
    std::map<long long, int> cid_counts;
    for (const auto &cr : rec.chunks) {
      cid_counts[cr.chunk_id]++;
    }
    for (auto &cr : rec.chunks) {
      if (cr.replica_count <= 0 || cr.replica_count != cid_counts[cr.chunk_id]) {
        cr.replica_count = cid_counts[cr.chunk_id];
      }
    }
    if (rec.replica_count <= 0 && !cid_counts.empty()) {
      rec.replica_count = cid_counts.begin()->second;
    }
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
    if (!kv.second.file_hash.empty()) {
        out << kv.first << " HASH=" << kv.second.file_hash << '\n';
    }
    if (kv.second.replica_count > 0) {
        out << kv.first << " REPLICAS=" << kv.second.replica_count << '\n';
    }
    for (const auto &chunk : kv.second.chunks) {
      out << kv.first << ' ' << chunk.chunk_id << ' ' << chunk.server << ' '
          << chunk.path << ' ' << chunk.checksum << ' ' << chunk.replica_count << '\n';
    }
  }
}

inline void replace_file_chunks(const std::string &file, const std::string& file_hash,
                                std::vector<ChunkRecord> &&chunks, int replica_count = 0) {
  std::lock_guard<std::mutex> lock(store_mutex());
  store()[file].file_hash = file_hash;

  // Calculate per-chunk replica counts if needed
  std::map<long long, int> cid_counts;
  for (const auto &cr : chunks) {
    cid_counts[cr.chunk_id]++;
  }
  for (auto &cr : chunks) {
    if (cr.replica_count <= 0) {
      cr.replica_count = cid_counts[cr.chunk_id];
    }
  }
  if (replica_count <= 0 && !cid_counts.empty()) {
    replica_count = cid_counts.begin()->second;
  }
  store()[file].replica_count = replica_count;
  store()[file].chunks = std::move(chunks);
  persist_locked();
}

inline FileRecord get_chunks(const std::string &file) {
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

inline bool atomic_replace_server_chunks(
    const std::string &dead_server,
    const std::string &new_server,
    const std::map<std::string, std::vector<ChunkRecord>> &file_chunks_to_update) {
  std::lock_guard<std::mutex> lock(store_mutex());
  for (const auto &[filename, new_chunks] : file_chunks_to_update) {
    auto it = store().find(filename);
    if (it == store().end()) continue;

    auto &chunks = it->second.chunks;
    // Remove dead server chunks
    chunks.erase(std::remove_if(chunks.begin(), chunks.end(),
        [&dead_server](const ChunkRecord &cr) {
          return cr.server == dead_server;
        }), chunks.end());

    // Add new server chunks
    for (const auto &nc : new_chunks) {
      chunks.push_back(nc);
    }

    std::map<long long, int> cid_counts;
    for (const auto &cr : chunks) {
      cid_counts[cr.chunk_id]++;
    }
    for (auto &cr : chunks) {
      cr.replica_count = cid_counts[cr.chunk_id];
    }
    if (!chunks.empty()) {
      it->second.replica_count = cid_counts.begin()->second;
    }
  }
  persist_locked();
  return true;
}

inline void delete_server_metadata(const std::string &server_address) {
  std::lock_guard<std::mutex> lock(store_mutex());
  for (auto &[filename, file_rec] : store()) {
    file_rec.chunks.erase(std::remove_if(file_rec.chunks.begin(), file_rec.chunks.end(),
        [&server_address](const ChunkRecord &cr) {
          return cr.server == server_address;
        }), file_rec.chunks.end());
    std::map<long long, int> cid_counts;
    for (const auto &cr : file_rec.chunks) {
      cid_counts[cr.chunk_id]++;
    }
    for (auto &cr : file_rec.chunks) {
      cr.replica_count = cid_counts[cr.chunk_id];
    }
  }
  persist_locked();
}
#endif
} // namespace metadata_store

#ifndef WITH_REDIS
using metadata_store::atomic_replace_server_chunks;
using metadata_store::delete_server_metadata;
inline std::vector<std::string> list_all_files() {
  return metadata_store::list_files_snapshot();
}
#endif

#ifdef WITH_REDIS
inline std::string get_redis_connection_string() {
  const char *host = std::getenv("REDIS_HOST");
  const char *port = std::getenv("REDIS_PORT");
  std::string host_str = host ? host : "127.0.0.1";
  std::string port_str = port ? port : "6379";
  return "tcp://" + host_str + ":" + port_str;
}

inline Redis& get_redis() {
  static Redis redis(get_redis_connection_string());
  return redis;
}

inline std::vector<std::string> list_all_files() {
  try {
    Redis &redis = get_redis();
    std::vector<std::string> files;
    long long cursor = 0;
    do {
      std::vector<std::string> batch;
      cursor = redis.scan(cursor, "file:*", 100, std::back_inserter(batch));
      for (const auto &key : batch) {
        if (key.rfind("file:", 0) == 0) {
          files.push_back(key.substr(5));
        }
      }
    } while (cursor != 0);

    std::sort(files.begin(), files.end());
    files.erase(std::unique(files.begin(), files.end()), files.end());
    return files;
  } catch (const std::exception &e) {
    std::cerr << "list_all_files error: " << e.what() << std::endl;
    return {};
  }
}

inline void create_entry(const std::string &request) {
  try {
    Redis &redis = get_redis(); // reused connection pool

    std::istringstream in(request);
    std::string file_name;
    std::getline(in, file_name);

    const std::string key = file_key(file_name);

    // Optional TTL line: "TTL=seconds", HASH=..., REPLICAS=...
    long long ttl = 0;
    std::string file_hash;
    int file_replica_count = 0;
    std::string line;
    
    while (std::getline(in, line)) {
      if (line.rfind("TTL=", 0) == 0) {
        ttl = std::stoll(line.substr(4));
      } else if (line.rfind("HASH=", 0) == 0) {
        file_hash = line.substr(5);
      } else if (line.rfind("REPLICAS=", 0) == 0) {
        try { file_replica_count = std::stoi(line.substr(9)); } catch (...) {}
      } else {
        // Break out of header parsing, need to parse this line as a chunk
        break;
      }
    }

    struct ParsedChunk {
      long long chunk_id;
      std::string server, path, checksum;
      int replica_count{0};
    };
    std::vector<ParsedChunk> parsed_chunks;

    auto parse_chunk_line = [&](const std::string& l) {
      if (l.empty()) return;
      std::istringstream ls(l);
      ParsedChunk pc;
      if (ls >> pc.chunk_id >> pc.server >> pc.path) {
        ls >> pc.checksum;
        if (!(ls >> pc.replica_count)) {
          pc.replica_count = 0;
        }
        parsed_chunks.push_back(pc);
      }
    };
    
    parse_chunk_line(line);
    while (std::getline(in, line)) {
      parse_chunk_line(line);
    }

    std::map<long long, int> cid_counts;
    for (const auto &pc : parsed_chunks) {
      cid_counts[pc.chunk_id]++;
    }

    // Batch fields for HSET key field value
    std::vector<std::pair<std::string, std::string>> fields;
    for (auto &pc : parsed_chunks) {
      if (pc.replica_count <= 0) {
        pc.replica_count = cid_counts[pc.chunk_id];
      }
      std::string field = "chunk:" + std::to_string(pc.chunk_id) + ":" + pc.server;
      std::string value = encode_loc(pc.server, pc.path) + "|" + pc.checksum + "|" + std::to_string(pc.replica_count);
      fields.emplace_back(field, value);
      fields.emplace_back("chunk_replicas:" + std::to_string(pc.chunk_id), std::to_string(pc.replica_count));
    }

    if (!fields.empty()) {
      redis.hset(key, fields.begin(), fields.end());
    }
    
    if (!file_hash.empty()) {
      redis.hset(key, "file_hash", file_hash);
    }

    if (file_replica_count <= 0 && !cid_counts.empty()) {
      file_replica_count = cid_counts.begin()->second;
    }
    if (file_replica_count > 0) {
      redis.hset(key, "replica_count", std::to_string(file_replica_count));
    }

    if (ttl > 0) {
      redis.expire(key, std::chrono::seconds{ttl});
    }

    std::cout << "Created file entry: " << file_name << " (replicas=" << file_replica_count << ")\n";
  } catch (const std::exception &e) {
    std::cerr << "create_entry error: " << e.what() << "\n";
  }
}
#else
inline void create_entry(const std::string &request) {
  std::istringstream in(request);
  std::string file_name;
  std::getline(in, file_name);

  std::vector<std::string> raw_lines;
  std::string line;
  while (std::getline(in, line)) {
    if (!line.empty())
      raw_lines.push_back(line);
  }

  if (!raw_lines.empty() && raw_lines.front().rfind("TTL=", 0) == 0) {
    raw_lines.erase(raw_lines.begin());
  }

  std::string file_hash;
  if (!raw_lines.empty() && raw_lines.front().rfind("HASH=", 0) == 0) {
    file_hash = raw_lines.front().substr(5);
    raw_lines.erase(raw_lines.begin());
  }

  int file_replica_count = 0;
  if (!raw_lines.empty() && raw_lines.front().rfind("REPLICAS=", 0) == 0) {
    try { file_replica_count = std::stoi(raw_lines.front().substr(9)); } catch (...) {}
    raw_lines.erase(raw_lines.begin());
  }

  std::vector<metadata_store::ChunkRecord> chunks;
  for (const auto &entry_line : raw_lines) {
    std::istringstream ls(entry_line);
    metadata_store::ChunkRecord record;
    if (ls >> record.chunk_id >> record.server >> record.path) {
        ls >> record.checksum;
        if (!(ls >> record.replica_count)) {
            record.replica_count = 0;
        }
        chunks.push_back(record);
    }
  }

  std::map<long long, int> cid_counts;
  for (const auto &cr : chunks) {
    cid_counts[cr.chunk_id]++;
  }
  for (auto &cr : chunks) {
    if (cr.replica_count <= 0) {
      cr.replica_count = cid_counts[cr.chunk_id];
    }
  }
  if (file_replica_count <= 0 && !cid_counts.empty()) {
    file_replica_count = cid_counts.begin()->second;
  }

  metadata_store::replace_file_chunks(file_name, file_hash, std::move(chunks), file_replica_count);
  std::cout << "Created file entry: " << file_name << " (replicas=" << file_replica_count << ")\n";
}
#endif

#ifdef WITH_REDIS
inline metadata_store::FileRecord query_metadata(const std::string &file_name) {
  metadata_store::FileRecord rec;
  if (file_name.empty()) return rec;
  try {
    const std::string key = file_key(file_name);
    Redis &redis = get_redis();
    std::unordered_map<std::string, std::string> all;
    redis.hgetall(key, std::inserter(all, all.end()));
    if (all.empty()) return rec;
    auto hash_it = all.find("file_hash");
    if (hash_it != all.end()) {
      rec.file_hash = hash_it->second;
    }
    auto rep_it = all.find("replica_count");
    if (rep_it != all.end()) {
      try { rec.replica_count = std::stoi(rep_it->second); } catch (...) {}
    }

    std::map<long long, int> per_chunk_rep;
    for (const auto &kv : all) {
      if (kv.first.rfind("chunk_replicas:", 0) == 0) {
        try {
          long long cid = std::stoll(kv.first.substr(15));
          per_chunk_rep[cid] = std::stoi(kv.second);
        } catch (...) {}
      }
    }

    for (const auto &kv : all) {
      if (kv.first.rfind("chunk:", 0) == 0) {
        metadata_store::ChunkRecord cr;
        try {
          std::string rest = kv.first.substr(6);
          size_t colon = rest.find(':');
          cr.chunk_id = std::stoll(colon == std::string::npos ? rest : rest.substr(0, colon));
        } catch (...) { continue; }
        std::string v = kv.second;
        auto p1 = v.find('|');
        if (p1 != std::string::npos) {
          cr.server = v.substr(0, p1);
          auto p2 = v.find('|', p1 + 1);
          if (p2 != std::string::npos) {
            cr.path = v.substr(p1 + 1, p2 - p1 - 1);
            auto p3 = v.find('|', p2 + 1);
            if (p3 != std::string::npos) {
              cr.checksum = v.substr(p2 + 1, p3 - p2 - 1);
              try { cr.replica_count = std::stoi(v.substr(p3 + 1)); } catch (...) { cr.replica_count = 1; }
            } else {
              cr.checksum = v.substr(p2 + 1);
              cr.replica_count = per_chunk_rep[cr.chunk_id] > 0 ? per_chunk_rep[cr.chunk_id] : 1;
            }
          } else {
            cr.path = v.substr(p1 + 1);
            cr.replica_count = 1;
          }
        } else {
          cr.server = v;
          cr.replica_count = 1;
        }
        rec.chunks.push_back(cr);
      }
    }

    // Set consistent replica counts
    std::map<long long, int> actual_counts;
    for (const auto &cr : rec.chunks) actual_counts[cr.chunk_id]++;
    for (auto &cr : rec.chunks) {
      if (cr.replica_count <= 0) cr.replica_count = actual_counts[cr.chunk_id];
    }
    if (rec.replica_count <= 0 && !actual_counts.empty()) {
      rec.replica_count = actual_counts.begin()->second;
    }
  } catch (const std::exception &e) {
    std::cerr << "query_metadata error: " << e.what() << "\n";
  }
  return rec;
}

inline void read_entry(const std::string &request) {
  std::istringstream in(request);
  std::string file_name;
  in >> file_name;
  if (file_name.empty()) {
    std::cerr << "read_entry: file_name required\n";
    return;
  }

  auto record = query_metadata(file_name);
  if (record.chunks.empty()) {
    std::cout << "No chunks or file not found\n";
    return;
  }

  if (!record.file_hash.empty()) {
    std::cout << "file_hash=" << record.file_hash << "\n";
  }
  std::cout << "replica_count=" << record.replica_count << "\n";

  long long desired_chunk = -1;
  if (in >> desired_chunk) {
    auto it = std::find_if(
        record.chunks.begin(), record.chunks.end(),
        [desired_chunk](const metadata_store::ChunkRecord &r) {
          return r.chunk_id == desired_chunk;
        });
    if (it != record.chunks.end()) {
      std::cout << "chunk:" << it->chunk_id << " server=" << it->server
                << " path=" << it->path;
      if (!it->checksum.empty()) std::cout << " checksum=" << it->checksum;
      std::cout << " replicas=" << it->replica_count << "\n";
    } else {
      std::cout << "Chunk not found\n";
    }
    return;
  }

  for (const auto &r : record.chunks) {
    std::cout << "chunk:" << r.chunk_id << " server=" << r.server
              << " path=" << r.path;
    if (!r.checksum.empty()) {
      std::cout << " checksum=" << r.checksum;
    }
    std::cout << " replicas=" << r.replica_count << "\n";
  }
}

inline bool atomic_replace_server_chunks(
    const std::string &dead_server,
    const std::string &new_server,
    const std::map<std::string, std::vector<metadata_store::ChunkRecord>> &file_chunks_to_update) {
  try {
    Redis &redis = get_redis();
    auto pipe = redis.pipeline();
    for (const auto &[filename, new_chunks] : file_chunks_to_update) {
      std::string key = file_key(filename);
      for (const auto &chunk : new_chunks) {
        std::string old_field = "chunk:" + std::to_string(chunk.chunk_id) + ":" + dead_server;
        pipe.hdel(key, old_field);

        std::string target_srv = !chunk.server.empty() ? chunk.server : new_server;
        std::string new_field = "chunk:" + std::to_string(chunk.chunk_id) + ":" + target_srv;
        std::string val = encode_loc(target_srv, chunk.path) + "|" + chunk.checksum + "|" + std::to_string(chunk.replica_count);
        pipe.hset(key, new_field, val);

        pipe.hset(key, "chunk_replicas:" + std::to_string(chunk.chunk_id), std::to_string(chunk.replica_count));
      }
    }
    pipe.exec();
    return true;
  } catch (const std::exception &e) {
    std::cerr << "atomic_replace_server_chunks error: " << e.what() << std::endl;
    return false;
  }
}

inline void delete_server_metadata(const std::string &server_address) {
  try {
    Redis &redis = get_redis();
    auto all_files = list_all_files();
    auto pipe = redis.pipeline();
    for (const auto &filename : all_files) {
      auto rec = query_metadata(filename);
      for (const auto &chunk : rec.chunks) {
        if (chunk.server == server_address) {
          pipe.hdel(file_key(filename), "chunk:" + std::to_string(chunk.chunk_id) + ":" + server_address);
        }
      }
    }
    pipe.exec();
  } catch (const std::exception &e) {
    std::cerr << "delete_server_metadata error: " << e.what() << std::endl;
  }
}
#else
inline metadata_store::FileRecord query_metadata(const std::string &file_name) {
  if (file_name.empty()) return {};
  return metadata_store::get_chunks(file_name);
}

inline void read_entry(const std::string &request) {
  std::istringstream in(request);
  std::string file_name;
  in >> file_name;
  if (file_name.empty()) {
    std::cerr << "read_entry: file_name required" << std::endl;
    return;
  }

  auto record = query_metadata(file_name);
  if (record.chunks.empty()) {
    std::cout << "No chunks or file not found" << std::endl;
    return;
  }

  if (!record.file_hash.empty()) {
      std::cout << "file_hash=" << record.file_hash << std::endl;
  }
  std::cout << "replica_count=" << record.replica_count << std::endl;

  long long desired_chunk = -1;
  if (in >> desired_chunk) {
    auto it = std::find_if(
        record.chunks.begin(), record.chunks.end(),
        [desired_chunk](const metadata_store::ChunkRecord &r) {
          return r.chunk_id == desired_chunk;
        });
    if (it != record.chunks.end()) {
      std::cout << "chunk:" << it->chunk_id << " server=" << it->server
                << " path=" << it->path << " checksum=" << it->checksum
                << " replicas=" << it->replica_count << std::endl;
    } else {
      std::cout << "Chunk not found" << std::endl;
    }
    return;
  }

  for (const auto &r : record.chunks) {
    std::cout << "chunk:" << r.chunk_id << " server=" << r.server
              << " path=" << r.path;
    if (!r.checksum.empty()) {
        std::cout << " checksum=" << r.checksum;
    }
    std::cout << " replicas=" << r.replica_count << std::endl;
  }
}
#endif

#ifdef WITH_REDIS
inline void delete_entry(const std::string &file_name) {
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
    Redis &redis = get_redis();

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
inline void delete_entry(const std::string &file_name) {
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
inline int create_replication(const std::string &ip_address) {
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

    Redis &redis = get_redis();
    redis.command("REPLICAOF", host,
                  std::to_string(port)); // server-side replication [19]
    return 0;
  } catch (const std::exception &e) {
    std::cerr << "create_replication error: " << e.what() << "\n";
    return -1;
  }
}
#else
inline int create_replication(const std::string &ip_address) {
  (void)ip_address;
  std::cout << "Redis disabled - replication simulated locally" << std::endl;
  return 0;
}
#endif

#ifdef WITH_REDIS
// Check if Redis is already running by trying to connect
inline bool is_redis_running(const std::string &host = "",
                             int port = 0) {
  try {
    std::string h = host;
    int p = port;
    if (h.empty()) {
      const char *env_h = std::getenv("REDIS_HOST");
      h = (env_h && *env_h) ? env_h : "127.0.0.1";
    }
    if (p <= 0) {
      const char *env_p = std::getenv("REDIS_PORT");
      p = (env_p && *env_p) ? std::stoi(env_p) : 6379;
    }
    Redis redis("tcp://" + h + ":" + std::to_string(p));
    redis.ping();
    return true;
  } catch (const std::exception &) {
    return false;
  }
}

inline int start_server() {
  // Check if Redis is already running
  if (is_redis_running()) {
    std::cout << "Redis server is already running, skipping daemon creation"
              << std::endl;
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

  umask(027);

  // Detach stdio and route log to safe location
  for (int fd = 0; fd < 3; ++fd)
    close(fd);
  open("/dev/null", O_RDONLY);                          // stdin
  open("/tmp/dfg_redis_server.log", O_WRONLY | O_CREAT | O_APPEND, 0644); // stdout
  open("/tmp/dfg_redis_server.log", O_WRONLY | O_CREAT | O_APPEND, 0644); // stderr

  // Find redis-server executable in PATH or standard paths
  const char *server_paths[] = {
      "redis-server",
      "/usr/bin/redis-server",
      "/usr/local/bin/redis-server",
      "/opt/homebrew/bin/redis-server"
  };

  for (const char *path : server_paths) {
      execlp(path, "redis-server", (char *)nullptr);
  }

  // If all exec attempts fail
  _exit(1);
}
#else
// Check if Redis would be running (always false when disabled)
inline bool is_redis_running(const std::string &host = "127.0.0.1",
                             int port = 6379) {
  (void)host;
  (void)port; // suppress unused parameter warnings
  return false;
}

inline int start_server() {
  std::cout << "Redis disabled - using in-memory storage simulation"
            << std::endl;
  return 0; // Return success since we're simulating
}
#endif

inline void start_daemon() {
  std::cout << "Starting Head Server daemon..." << std::endl;

#ifdef WITH_REDIS
  // Check if Redis is already running first
  if (is_redis_running()) {
    std::cout << "Redis server is already running, proceeding with Head Server "
                 "startup"
              << std::endl;
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
  std::cout << "Redis disabled - using in-memory storage simulation"
            << std::endl;
#endif

  std::cout << "Head Server daemon is ready" << std::endl;
}

#endif // REDIS_HANDLER_HPP
