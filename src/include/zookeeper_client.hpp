#pragma once

#include <zookeeper.h>
#include <string>
#include <functional>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <chrono>

class ZooKeeperClient {
public:
    using WatchCallback = std::function<void(int type, int state, const char* path)>;
    using StringCallback = std::function<void(int rc, const char* value, int value_len)>;
    using StringsCallback = std::function<void(int rc, const String_vector* strings)>;
    using StatCallback = std::function<void(int rc, const struct Stat* stat)>;
    using DataCallback = std::function<void(int rc, const char* value, int value_len, const struct Stat* stat)>;

    ZooKeeperClient(const std::string& hosts, int recv_timeout = 10000);
    ~ZooKeeperClient();

    // Connection management
    bool connect();
    void disconnect();
    bool is_connected() const;
    int get_state() const;
    std::string get_current_server() const;

    // Synchronous operations
    int create_node(const std::string& path, const std::string& data, bool ephemeral = false, bool sequence = false);
    int delete_node(const std::string& path, int version = -1);
    bool node_exists(const std::string& path);
    std::string get_node_data(const std::string& path, bool watch = false);
    int set_node_data(const std::string& path, const std::string& data, int version = -1);
    std::vector<std::string> get_children(const std::string& path, bool watch = false);
    
    // Asynchronous operations
    void create_node_async(const std::string& path, const std::string& data, 
                          bool ephemeral, bool sequence, StringCallback cb);
    void get_children_async(const std::string& path, bool watch, StringsCallback cb);
    void get_node_data_async(const std::string& path, bool watch, DataCallback cb);
    void set_node_data_async(const std::string& path, const std::string& data, 
                            int version, StatCallback cb);

    // Watch management
    void set_watch_callback(WatchCallback cb);

private:
    static void watcher(zhandle_t* zh, int type, int state, const char* path, void* watcherCtx);
    static void string_completion(int rc, const char* value, const void* data);
    static void strings_completion(int rc, const struct String_vector* strings, const void* data);
    static void stat_completion(int rc, const struct Stat* stat, const void* data);
    static void data_completion(int rc, const char* value, int value_len, 
                              const struct Stat* stat, const void* data);

    zhandle_t* zk_handle_;
    std::string hosts_;
    int recv_timeout_;
    WatchCallback watch_callback_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    bool connected_;
};
