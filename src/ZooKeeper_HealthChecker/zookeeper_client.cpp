#include "zookeeper_client.hpp"
#include <cstring>
#include <stdexcept>
#include <sstream>
#include <iostream>

ZooKeeperClient::ZooKeeperClient(const std::string& hosts, int recv_timeout)
    : hosts_(hosts), recv_timeout_(recv_timeout), connected_(false), zk_handle_(nullptr) {
}

ZooKeeperClient::~ZooKeeperClient() {
    disconnect();
}

bool ZooKeeperClient::connect() {
    std::unique_lock<std::mutex> lock(mutex_);
    
    if (zk_handle_) {
        return true;
    }
    
    // Initialize ZooKeeper client
    zk_handle_ = zookeeper_init(hosts_.c_str(), 
                               &ZooKeeperClient::watcher, 
                               recv_timeout_, 
                               nullptr, 
                               this, 
                               0);
    
    if (!zk_handle_) {
        return false;
    }
    
    // Wait for connection to be established
    if (cv_.wait_for(lock, std::chrono::milliseconds(recv_timeout_), 
                    [this] { return connected_ || !zk_handle_; })) {
        return connected_;
    }
    
    return false;
}

void ZooKeeperClient::disconnect() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (zk_handle_) {
        zookeeper_close(zk_handle_);
        zk_handle_ = nullptr;
        connected_ = false;
    }
}

bool ZooKeeperClient::is_connected() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return connected_ && zk_handle_ && zoo_state(zk_handle_) == ZOO_CONNECTED_STATE;
}

int ZooKeeperClient::get_state() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return zk_handle_ ? zoo_state(zk_handle_) : 0;
}

std::string ZooKeeperClient::get_current_server() const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!zk_handle_) return "";
    
    char buf[1024] = {0};
    int len = sizeof(buf);
    zoo_get_current_server(zk_handle_, buf, &len);
    return std::string(buf, len);
}

// Synchronous operations
int ZooKeeperClient::create_node(const std::string& path, const std::string& data, bool ephemeral, bool sequence) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!zk_handle_) return ZINVALIDSTATE;
    
    int flags = 0;
    if (ephemeral) flags |= ZOO_EPHEMERAL;
    if (sequence) flags |= ZOO_SEQUENCE;
    
    char path_buffer[1024];
    int path_len = sizeof(path_buffer);
    
    int rc = zoo_create(zk_handle_, 
                       path.c_str(), 
                       data.c_str(), 
                       data.length(),
                       &ZOO_OPEN_ACL_UNSAFE,
                       flags,
                       path_buffer,
                       path_len);
    
    return rc;
}

int ZooKeeperClient::delete_node(const std::string& path, int version) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!zk_handle_) return ZINVALIDSTATE;
    
    return zoo_delete(zk_handle_, path.c_str(), version);
}

bool ZooKeeperClient::node_exists(const std::string& path) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!zk_handle_) return false;
    
    struct Stat stat;
    return zoo_exists(zk_handle_, path.c_str(), 0, &stat) == ZOK;
}

std::string ZooKeeperClient::get_node_data(const std::string& path, bool watch) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!zk_handle_) return "";
    
    char buffer[1024 * 1024]; // 1MB buffer
    int buffer_len = sizeof(buffer);
    struct Stat stat;
    
    int rc = zoo_wget(zk_handle_, 
                     path.c_str(), 
                     watch ? &ZooKeeperClient::watcher : nullptr,
                     this,
                     buffer, 
                     &buffer_len,
                     &stat);
    
    if (rc == ZOK) {
        return std::string(buffer, buffer_len);
    }
    
    return "";
}

int ZooKeeperClient::set_node_data(const std::string& path, const std::string& data, int version) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!zk_handle_) return ZINVALIDSTATE;
    
    return zoo_set(zk_handle_, path.c_str(), data.c_str(), data.length(), version);
}

std::vector<std::string> ZooKeeperClient::get_children(const std::string& path, bool watch) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::string> children;
    
    if (!zk_handle_) return children;
    
    struct String_vector strings;
    int rc = zoo_wget_children(zk_handle_, 
                              path.c_str(), 
                              watch ? &ZooKeeperClient::watcher : nullptr,
                              this,
                              &strings);
    
    if (rc == ZOK) {
        for (int i = 0; i < strings.count; ++i) {
            children.emplace_back(strings.data[i]);
        }
        deallocate_String_vector(&strings);
    }
    
    return children;
}

// Asynchronous operations
void ZooKeeperClient::create_node_async(const std::string& path, 
                                       const std::string& data, 
                                       bool ephemeral, 
                                       bool sequence, 
                                       StringCallback cb) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!zk_handle_) {
        cb(ZINVALIDSTATE, nullptr, 0);
        return;
    }
    
    int flags = 0;
    if (ephemeral) flags |= ZOO_EPHEMERAL;
    if (sequence) flags |= ZOO_SEQUENCE;
    
    auto* ctx = new StringCallback(std::move(cb));
    
    int rc = zoo_acreate(zk_handle_, 
                        path.c_str(), 
                        data.c_str(), 
                        data.length(),
                        &ZOO_OPEN_ACL_UNSAFE,
                        flags,
                        &ZooKeeperClient::string_completion,
                        ctx);
    
    if (rc != ZOK) {
        delete ctx;
        cb(rc, nullptr, 0);
    }
}

void ZooKeeperClient::get_children_async(const std::string& path, bool watch, StringsCallback cb) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!zk_handle_) {
        cb(ZINVALIDSTATE, nullptr);
        return;
    }
    
    auto* ctx = new StringsCallback(std::move(cb));
    
    int rc = zoo_awget_children(zk_handle_,
                               path.c_str(),
                               watch ? &ZooKeeperClient::watcher : nullptr,
                               this,
                               &ZooKeeperClient::strings_completion,
                               ctx);
    
    if (rc != ZOK) {
        delete ctx;
        cb(rc, nullptr);
    }
}

void ZooKeeperClient::get_node_data_async(const std::string& path, bool watch, DataCallback cb) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!zk_handle_) {
        cb(ZINVALIDSTATE, nullptr, 0, nullptr);
        return;
    }
    
    auto* ctx = new DataCallback(std::move(cb));
    
    int rc = zoo_awget(zk_handle_,
                      path.c_str(),
                      watch ? &ZooKeeperClient::watcher : nullptr,
                      this,
                      &ZooKeeperClient::data_completion,
                      ctx);
    
    if (rc != ZOK) {
        delete ctx;
        cb(rc, nullptr, 0, nullptr);
    }
}

void ZooKeeperClient::set_node_data_async(const std::string& path, 
                                         const std::string& data, 
                                         int version, 
                                         StatCallback cb) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!zk_handle_) {
        cb(ZINVALIDSTATE, nullptr);
        return;
    }
    
    auto* ctx = new StatCallback(std::move(cb));
    
    int rc = zoo_aset(zk_handle_,
                     path.c_str(),
                     data.c_str(),
                     data.length(),
                     version,
                     &ZooKeeperClient::stat_completion,
                     ctx);
    
    if (rc != ZOK) {
        delete ctx;
        cb(rc, nullptr);
    }
}

// Watch management
void ZooKeeperClient::set_watch_callback(WatchCallback cb) {
    std::lock_guard<std::mutex> lock(mutex_);
    watch_callback_ = std::move(cb);
}

// Static callbacks
void ZooKeeperClient::watcher(zhandle_t* zh, int type, int state, const char* path, void* watcherCtx) {
    auto* client = static_cast<ZooKeeperClient*>(watcherCtx);
    if (!client) return;
    
    {
        std::lock_guard<std::mutex> lock(client->mutex_);
        if (state == ZOO_CONNECTED_STATE) {
            client->connected_ = true;
            client->cv_.notify_all();
        } else if (state == ZOO_EXPIRED_SESSION_STATE) {
            client->connected_ = false;
            // Attempt to reconnect
            client->connect();
        }
    }
    
    if (client->watch_callback_) {
        client->watch_callback_(type, state, path);
    }
}

void ZooKeeperClient::string_completion(int rc, const char* value, const void* data) {
    auto* cb = const_cast<StringCallback*>(static_cast<const StringCallback*>(data));
    if (cb) {
        (*cb)(rc, value, value ? strlen(value) : 0);
        delete cb;
    }
}

void ZooKeeperClient::strings_completion(int rc, const struct String_vector* strings, const void* data) {
    auto* cb = const_cast<StringsCallback*>(static_cast<const StringsCallback*>(data));
    if (cb) {
        (*cb)(rc, strings);
        delete cb;
    }
}

void ZooKeeperClient::stat_completion(int rc, const struct Stat* stat, const void* data) {
    auto* cb = const_cast<StatCallback*>(static_cast<const StatCallback*>(data));
    if (cb) {
        (*cb)(rc, stat);
        delete cb;
    }
}

void ZooKeeperClient::data_completion(int rc, const char* value, int value_len, 
                                     const struct Stat* stat, const void* data) {
    auto* cb = const_cast<DataCallback*>(static_cast<const DataCallback*>(data));
    if (cb) {
        (*cb)(rc, value, value_len, stat);
        delete cb;
    }
}
