#pragma once

#include <atomic>
#include <chrono>
#include <dfg/atomic_output.hpp>
#include <dfg/config_loader.hpp>
#include <dfg/protocol.hpp>
#include <dfg/thread_pool.hpp>
#include <fcntl.h>
#include <filesystem>
#include <future>
#include <iostream>
#include <map>
#include <mutex>
#include <sstream>
#include <string>
#include <unistd.h>
#include <unordered_map>
#include <vector>

// Global I/O thread pool (shared by all storage operations)
inline dfg::ThreadPool &io_pool() {
  static dfg::ThreadPool pool(4);
  return pool;
}

namespace fs = std::filesystem;

// Chunk Storage
class ChunkStorage {
private:
  std::string storage_path = "/var/cluster_storage/";
  std::unordered_map<std::string, std::string> chunk_registry;
  std::mutex registry_mutex;

  int ensure_storage_directory() {
    std::error_code ec;
    fs::create_directories(storage_path, ec);
    if (ec || ::access(storage_path.c_str(), W_OK) != 0) {
      // Fallback to writable temporary storage path
      storage_path = "/tmp/cluster_storage/";
      fs::create_directories(storage_path, ec);
      if (ec || ::access(storage_path.c_str(), W_OK) != 0) {
        std::cerr << "Failed to create or access storage directory: "
                  << storage_path << std::endl;
        return -1;
      }
    }
    return 0;
  }

  std::string generate_chunk_path(const std::string &chunk_id) {
    static std::atomic<uint64_t> seq{0};
    auto now = std::chrono::system_clock::now();
    auto epoch_us = std::chrono::duration_cast<std::chrono::microseconds>(
                        now.time_since_epoch())
                        .count();
    std::stringstream ss;
    ss << storage_path << "chunk_" << chunk_id << "_" << epoch_us << "_"
       << (++seq) << ".dat";
    return ss.str();
  }

public:
  explicit ChunkStorage(std::string directory = "") {
    storage_path = directory.empty() ? cluster_server_config().get_string(
                                           "storage.chunk_dir", storage_path)
                                     : std::move(directory);
    if (!storage_path.empty() && storage_path.back() != '/') {
      storage_path += '/';
    }
    int status = ensure_storage_directory();
    if (status != 0) {
      throw std::runtime_error("Error creating storage directory");
    }
    // Rebuild the public lookup after a process restart. Metadata paths alone
    // do not help direct GET_CHUNK clients locate files.
    std::map<std::string, fs::file_time_type> newest;
    for (const auto &entry : fs::directory_iterator(storage_path)) {
      if (!entry.is_regular_file() || entry.is_symlink())
        continue;
      const std::string name = entry.path().filename().string();
      if (!name.starts_with("chunk_") || !name.ends_with(".dat"))
        continue;
      const auto sequence = name.rfind('_');
      const auto timestamp = name.rfind('_', sequence - 1);
      if (timestamp == std::string::npos || timestamp <= 6)
        continue;
      const std::string id = name.substr(6, timestamp - 6);
      uint64_t time_value = 0, sequence_value = 0;
      if (!dfg::protocol::valid_name(id, 240) ||
          !dfg::protocol::parse_size(
              name.substr(timestamp + 1, sequence - timestamp - 1),
              time_value) ||
          !dfg::protocol::parse_size(
              name.substr(sequence + 1, name.size() - sequence - 5),
              sequence_value))
        continue;
      const auto modified = entry.last_write_time();
      if (!newest.contains(id) || modified > newest[id]) {
        newest[id] = modified;
        chunk_registry[id] = entry.path().string();
      }
    }
  }

  // Async store: offload to I/O thread pool
  std::future<std::pair<bool, std::string>>
  async_store_chunk(const std::string &chunk_id, std::vector<char> data) {
    if (!dfg::protocol::valid_name(chunk_id, 240) ||
        data.size() > dfg::protocol::max_chunk_bytes) {
      std::promise<std::pair<bool, std::string>> failure;
      failure.set_value({false, ""});
      return failure.get_future();
    }
    auto path = generate_chunk_path(chunk_id);
    return io_pool().submit(
        [this, chunk_id, path,
         data = std::move(data)]() -> std::pair<bool, std::string> {
          try {
            dfg::AtomicOutput output(path);
            output.stream().write(data.data(), data.size());
            output.commit();
            std::lock_guard lock(registry_mutex);
            chunk_registry[chunk_id] = path;
            return {true, path};
          } catch (const std::exception &error) {
            std::cerr << "Cannot store chunk " << chunk_id << ": "
                      << error.what() << std::endl;
            return {false, ""};
          }
        });
  }

  // Async retrieve: offload to I/O thread pool
  std::future<std::vector<char>>
  async_retrieve_chunk(const std::string &chunk_id,
                       const std::string &fallback_path = "") {
    std::string chunk_path;
    {
      std::lock_guard<std::mutex> lock(registry_mutex);
      auto it = chunk_registry.find(chunk_id);
      std::error_code error;
      auto candidate = fs::weakly_canonical(fallback_path, error);
      const auto root = fs::canonical(storage_path);
      if (!fallback_path.empty() && !error && candidate.parent_path() == root &&
          fs::is_regular_file(candidate)) {
        chunk_path = candidate.string();
      } else if (fallback_path.empty() && it != chunk_registry.end()) {
        chunk_path = it->second;
      } else {
        // Return an immediately-ready future with empty data.
        std::promise<std::vector<char>> p;
        p.set_value({});
        return p.get_future();
      }
    }

    return io_pool().submit([chunk_id, chunk_path]() -> std::vector<char> {
      int fd = ::open(chunk_path.c_str(), O_RDONLY | O_CLOEXEC);
      if (fd < 0) {
        std::cerr << "Failed to open chunk file: " << chunk_path << std::endl;
        return {};
      }

      off_t file_size = ::lseek(fd, 0, SEEK_END);
      if (file_size < 0) {
        std::cerr << "Failed to seek chunk file: " << chunk_path << std::endl;
        ::close(fd);
        return {};
      }
      ::lseek(fd, 0, SEEK_SET);

      if (static_cast<uint64_t>(file_size) > dfg::protocol::max_chunk_bytes) {
        ::close(fd);
        return {};
      }
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
      if (total_read != data.size())
        return {};
      return data;
    });
  }

  // Idempotent deletion lets a failed multi-replica delete be retried.
  // Keep the registry entry until the filesystem operation succeeds.
  bool delete_chunk(const std::string &chunk_id) {
    std::lock_guard lock(registry_mutex);
    auto entry = chunk_registry.find(chunk_id);
    if (entry == chunk_registry.end())
      return true;
    std::error_code error;
    fs::remove(entry->second, error);
    if (error) {
      std::cerr << "Cannot delete chunk " << chunk_id << ": " << error.message()
                << std::endl;
      return false;
    }
    chunk_registry.erase(entry);
    return true;
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
