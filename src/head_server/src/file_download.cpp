#include "file_download.hpp"

#include "file_transfer.pb.h"
#include "server_state.hpp"

#include <algorithm>
#include <dfg/metadata/store.hpp>
#include <dfg/net_utils.hpp>
#include <dfg/protocol.hpp>
#include <dfg/sha256.hpp>
#include <dfg/thread_pool.hpp>
#include <dfg/unique_fd.hpp>
#include <future>
#include <iostream>
#include <map>
#include <string>
#include <vector>

// Replica verification, download locations, and repair transfers
static constexpr int MAX_CONCURRENT_FETCHES = 6;

struct ChunkLocation {
  int chunk_id;
  std::string server_ip;
  std::string file_path;
};

static dfg::ThreadPool &fetch_pool() {
  static dfg::ThreadPool pool(MAX_CONCURRENT_FETCHES);
  return pool;
}

class FileReconstructor {
public:
  std::pair<std::vector<ChunkLocation>, std::string>
  get_chunk_locations_from_metadata(const std::string &filename) {
    std::vector<ChunkLocation> locations;
    std::string file_hash;

    try {
      auto record = query_metadata(filename);
      file_hash = record.file_hash;
      for (const auto &chunk : record.chunks) {
        ChunkLocation loc;
        loc.chunk_id = static_cast<int>(chunk.chunk_id);
        loc.server_ip = chunk.server;
        loc.file_path = chunk.path;
        locations.push_back(loc);
      }
    } catch (const std::exception &e) {
      std::cerr << "Error getting chunk locations: " << e.what() << std::endl;
    }

    return {locations, file_hash};
  }

  file_transfer::v1::FetchHashResponse
  get_chunk_hash(const ChunkLocation &location, const std::string &filename) {
    file_transfer::v1::FetchHashResponse resp;
    resp.set_success(false);

    std::string ip;
    int port;
    dfg::net::parse_address(location.server_ip, ip, port);

    int transfer_port = 8180;
    if (g_cluster_registry) {
      for (const auto &s : g_cluster_registry->get_all()) {
        if (s.address() == location.server_ip) {
          if (s.transfer_port > 0)
            transfer_port = s.transfer_port;
          break;
        }
      }
    }

    int sock = dfg::net::connect_with_timeout(ip, transfer_port);
    dfg::UniqueFd connection(sock);
    if (sock < 0)
      return resp;

    file_transfer::v1::FetchHashRequest req;
    std::string unique_chunk_id =
        filename + "_chunk_" + std::to_string(location.chunk_id);
    req.set_chunk_id(unique_chunk_id);
    req.set_file_path(location.file_path);

    std::string serialized;
    req.SerializeToString(&serialized);
    uint32_t type = htonl(3);
    uint32_t len = htonl(serialized.size());

    if (!dfg::net::send_all(sock, &type, sizeof(type)) ||
        !dfg::net::send_all(sock, &len, sizeof(len)) ||
        !dfg::net::send_all(sock, serialized.data(), serialized.size())) {
      connection.reset();
      return resp;
    }

    uint32_t resp_len_net;
    if (!dfg::net::recv_all(sock, &resp_len_net, sizeof(resp_len_net))) {
      connection.reset();
      return resp;
    }

    uint32_t resp_len = ntohl(resp_len_net);
    if (resp_len == 0 || resp_len > dfg::protocol::max_message_bytes)
      throw std::runtime_error("Invalid chunk response length");
    std::vector<char> resp_buf(resp_len);

    if (!dfg::net::recv_all(sock, resp_buf.data(), resp_len)) {
      connection.reset();
      return resp;
    }

    resp.ParseFromArray(resp_buf.data(), resp_len);
    connection.reset();
    return resp;
  }

public:
  bool repair_chunk_on_server(const ChunkLocation &location,
                              const std::string &filename,
                              const std::vector<char> &chunk_data,
                              std::string *out_file_path = nullptr) {
    std::string ip;
    int port;
    dfg::net::parse_address(location.server_ip, ip, port);

    int transfer_port = 8180;
    if (g_cluster_registry) {
      for (const auto &s : g_cluster_registry->get_all()) {
        if (s.address() == location.server_ip) {
          if (s.transfer_port > 0)
            transfer_port = s.transfer_port;
          break;
        }
      }
    }

    int sock = dfg::net::connect_with_timeout(ip, transfer_port);
    dfg::UniqueFd connection(sock);
    if (sock < 0)
      return false;

    file_transfer::v1::ChunkData msg;
    std::string unique_chunk_id =
        filename + "_chunk_" + std::to_string(location.chunk_id);
    msg.set_chunk_id(unique_chunk_id);
    msg.set_filename(filename);
    msg.set_data(chunk_data.data(), chunk_data.size());

    std::string serialized;
    msg.SerializeToString(&serialized);

    uint32_t type = htonl(1);
    uint32_t len = htonl(serialized.size());

    if (!dfg::net::send_all(sock, &type, sizeof(type)) ||
        !dfg::net::send_all(sock, &len, sizeof(len)) ||
        !dfg::net::send_all(sock, serialized.data(), serialized.size())) {
      connection.reset();
      return false;
    }

    uint32_t resp_len_net;
    if (dfg::net::recv_all(sock, &resp_len_net, sizeof(resp_len_net))) {
      uint32_t resp_len = ntohl(resp_len_net);
      if (resp_len == 0 || resp_len > dfg::protocol::max_message_bytes)
        throw std::runtime_error("Invalid chunk response length");
      std::vector<char> resp_buf(resp_len);
      if (dfg::net::recv_all(sock, resp_buf.data(), resp_len)) {
        file_transfer::v1::ChunkResponse resp;
        if (resp.ParseFromArray(resp_buf.data(), resp_len) && resp.success()) {
          if (out_file_path) {
            *out_file_path = resp.file_path();
          }
          connection.reset();
          return true;
        }
      }
    }
    connection.reset();
    return false;
  }

  std::vector<char> read_chunk_from_server(const ChunkLocation &location,
                                           const std::string &filename) {
    std::vector<char> chunk_data;

    std::string ip;
    int port;
    dfg::net::parse_address(location.server_ip, ip, port);

    int transfer_port = 8180;
    if (g_cluster_registry) {
      for (const auto &s : g_cluster_registry->get_all()) {
        if (s.address() == location.server_ip) {
          if (s.transfer_port > 0)
            transfer_port = s.transfer_port;
          break;
        }
      }
    }

    int sock = dfg::net::connect_with_timeout(ip, transfer_port);
    dfg::UniqueFd connection(sock);
    if (sock < 0)
      return chunk_data;

    file_transfer::v1::FetchChunkRequest req;
    std::string unique_chunk_id =
        filename + "_chunk_" + std::to_string(location.chunk_id);
    req.set_chunk_id(unique_chunk_id);
    req.set_file_path(location.file_path);

    std::string serialized;
    req.SerializeToString(&serialized);

    uint32_t type = htonl(2);
    uint32_t len = htonl(serialized.size());

    if (!dfg::net::send_all(sock, &type, sizeof(type)) ||
        !dfg::net::send_all(sock, &len, sizeof(len)) ||
        !dfg::net::send_all(sock, serialized.data(), serialized.size())) {
      connection.reset();
      return chunk_data;
    }

    uint32_t resp_len_net;
    if (!dfg::net::recv_all(sock, &resp_len_net, sizeof(resp_len_net))) {
      connection.reset();
      return chunk_data;
    }

    uint32_t resp_len = ntohl(resp_len_net);
    if (resp_len == 0 || resp_len > dfg::protocol::max_message_bytes)
      throw std::runtime_error("Invalid chunk response length");
    std::vector<char> resp_buf(resp_len);

    if (!dfg::net::recv_all(sock, resp_buf.data(), resp_len)) {
      connection.reset();
      return chunk_data;
    }

    file_transfer::v1::FetchChunkResponse resp;
    if (!resp.ParseFromArray(resp_buf.data(), resp_len) || !resp.success()) {
      std::cerr << "Server rejected fetch: " << resp.error_message()
                << std::endl;
      connection.reset();
      return chunk_data;
    }

    chunk_data.assign(resp.data().begin(), resp.data().end());
    connection.reset();
    std::cout << "Read chunk " << location.chunk_id << " (" << chunk_data.size()
              << " bytes) from " << location.server_ip << std::endl;
    return chunk_data;
  }

  bool send_download_locations(const std::string &filename, int fd) {
    auto [chunk_locations, file_hash] =
        get_chunk_locations_from_metadata(filename);
    if (chunk_locations.empty()) {
      if (!file_hash.empty()) {
        std::string header = "FILE_HASH " + file_hash + "\nEOF\n";
        dfg::net::send_all(fd, header.data(), header.size());
        return true;
      }
      std::string err = "ERROR: No chunks found for file\n";
      dfg::net::send_all(fd, err.data(), err.size());
      return false;
    }

    std::map<int, std::vector<ChunkLocation>> chunk_replicas;
    for (const auto &location : chunk_locations) {
      chunk_replicas[location.chunk_id].push_back(location);
    }

    std::string hash_msg = "FILE_HASH " + file_hash + "\n";
    dfg::net::send_all(fd, hash_msg.data(), hash_msg.size());

    std::string count_msg =
        "CHUNKS " + std::to_string(chunk_replicas.size()) + "\n";
    dfg::net::send_all(fd, count_msg.data(), count_msg.size());

    struct HashResult {
      int chunk_id;
      ChunkLocation location;
      file_transfer::v1::FetchHashResponse response;
    };

    std::vector<std::future<HashResult>> hash_futures;
    for (const auto &[chunk_id, replicas] : chunk_replicas) {
      for (const auto &replica : replicas) {
        auto cid = chunk_id;
        auto loc = replica;
        auto fname = filename;
        hash_futures.push_back(
            fetch_pool().submit([this, cid, loc, fname]() -> HashResult {
              auto resp = get_chunk_hash(loc, fname);
              return HashResult{cid, loc, resp};
            }));
      }
    }

    struct ServerInfo {
      ChunkLocation loc;
      float load_score;
      int active_connections;
    };
    struct ChunkConsensus {
      std::map<std::string, int> hash_counts;
      std::map<std::string, std::vector<ServerInfo>> hash_to_servers;
    };
    std::map<int, ChunkConsensus> consensus_map;

    for (auto &fut : hash_futures) {
      auto result = fut.get();
      auto &consensus = consensus_map[result.chunk_id];
      if (result.response.success()) {
        consensus.hash_counts[result.response.hash()]++;
        consensus.hash_to_servers[result.response.hash()].push_back(
            {result.location, result.response.load_score(),
             result.response.active_connections()});
      }
    }

    struct ChunkFetchPlan {
      int chunk_id;
      ChunkLocation best_server;
      std::string majority_hash;
    };

    std::vector<ChunkFetchPlan> fetch_plans;
    for (const auto &[chunk_id, consensus] : consensus_map) {
      if (consensus.hash_counts.empty()) {
        std::string err = "ERROR: Could not verify hashes for chunk " +
                          std::to_string(chunk_id) + "\n";
        dfg::net::send_all(fd, err.data(), err.size());
        return false;
      }
      std::string majority_hash;
      int max_count = 0;
      for (const auto &[hash, count] : consensus.hash_counts) {
        if (count > max_count) {
          max_count = count;
          majority_hash = hash;
        }
      }
      auto majority_servers = consensus.hash_to_servers.at(majority_hash);
      std::sort(majority_servers.begin(), majority_servers.end(),
                [](const auto &a, const auto &b) {
                  if (a.active_connections != b.active_connections)
                    return a.active_connections < b.active_connections;
                  return a.load_score < b.load_score;
                });
      fetch_plans.push_back(ChunkFetchPlan{
          chunk_id, majority_servers.front().loc, majority_hash});
    }

    auto servers = g_cluster_registry->get_all();

    for (const auto &plan : fetch_plans) {
      std::string server_ip = plan.best_server.server_ip;
      int public_port = 8080; // fallback
      for (const auto &s : servers) {
        if (s.address() == server_ip) {
          public_port = s.public_port;
          break;
        }
      }

      std::string ip_only = server_ip.substr(0, server_ip.find(':'));

      std::string loc_msg = "CHUNK_LOC " + std::to_string(plan.chunk_id) + " " +
                            ip_only + " " + std::to_string(public_port) + " " +
                            plan.majority_hash + "\n";
      dfg::net::send_all(fd, loc_msg.data(), loc_msg.size());
    }
    std::string eof = "FILE_HASH " + file_hash + "\nEOF\n";
    dfg::net::send_all(fd, eof.data(), eof.size());
    return true;
  }
};

static FileReconstructor g_file_reconstructor;

void handle_client_delete(int fd, const std::string &filename) {
  auto [chunk_locations, file_hash] =
      g_file_reconstructor.get_chunk_locations_from_metadata(filename);
  if (chunk_locations.empty() && file_hash.empty()) {
    std::string err = "ERROR: File not found\n";
    dfg::net::send_all(fd, err.data(), err.size());
    return;
  }

  bool all_deleted = true;
  for (const auto &loc : chunk_locations) {
    std::string ip;
    int port;
    dfg::net::parse_address(loc.server_ip, ip, port);

    int sock = dfg::net::connect_with_timeout(ip, port, 5);
    dfg::UniqueFd connection(sock);
    if (sock < 0) {
      all_deleted = false;
      continue;
    }
    const std::string chunk_id =
        filename + "_chunk_" + std::to_string(loc.chunk_id);
    const std::string request = "DELETE_CHUNK " + chunk_id + "\n";
    std::string reply;
    if (!dfg::net::send_all(sock, request.data(), request.size()) ||
        !dfg::net::recv_line(sock, reply) || reply != "SUCCESS")
      all_deleted = false;
  }

  if (all_deleted) {
    delete_entry(filename);
    std::string ok = "SUCCESS\n";
    dfg::net::send_all(fd, ok.data(), ok.size());
  } else {
    std::string err = "ERROR: Could not delete chunks\n";
    dfg::net::send_all(fd, err.data(), err.size());
  }
}

void handle_client_download(int fd, const std::string &filename) {

  g_file_reconstructor.send_download_locations(filename, fd);
}

bool replicate_chunk_to_server(const std::string &source_server, int chunk_id,
                               const std::string &target_server,
                               const std::string &filename,
                               std::string *out_file_path,
                               const std::string &source_file_path) {
  ChunkLocation source_loc;
  source_loc.chunk_id = chunk_id;
  source_loc.server_ip = source_server;
  source_loc.file_path = source_file_path;

  auto chunk_data =
      g_file_reconstructor.read_chunk_from_server(source_loc, filename);
  if (chunk_data.empty()) {
    std::cerr << "[Replication] Failed to read chunk " << chunk_id
              << " from surviving server " << source_server << std::endl;
    return false;
  }

  ChunkLocation target_loc;
  target_loc.chunk_id = chunk_id;
  target_loc.server_ip = target_server;
  target_loc.file_path = "/tmp/chunks/" + target_server + "_" + filename +
                         "_chunk_" + std::to_string(chunk_id);

  bool ok = g_file_reconstructor.repair_chunk_on_server(
      target_loc, filename, chunk_data, out_file_path);
  if (ok) {
    std::cout << "[Replication] Successfully duplicated chunk " << chunk_id
              << " of file " << filename << " to new server " << target_server
              << std::endl;
  } else {
    std::cerr << "[Replication] Failed to write chunk " << chunk_id
              << " to new server " << target_server << std::endl;
  }
  return ok;
}
