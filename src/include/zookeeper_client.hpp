#pragma once

#include <functional>
#include <mutex>
#include <string>
#include <vector>

// Minimal constants to keep downstream code compatible with the original
// ZooKeeper C API. Values match the upstream enumeration so existing logs and
// conditionals behave the same way.
constexpr int ZOK = 0;
constexpr int ZNODEEXISTS = -110;
constexpr int ZNONODE = -101;
constexpr int ZINVALIDSTATE = -112;

constexpr int ZOO_CREATED_EVENT = 1;
constexpr int ZOO_DELETED_EVENT = 2;
constexpr int ZOO_CHANGED_EVENT = 3;
constexpr int ZOO_CHILD_EVENT = 4;

constexpr int ZOO_CONNECTING_STATE = 1;
constexpr int ZOO_ASSOCIATING_STATE = 2;
constexpr int ZOO_CONNECTED_STATE = 3;
constexpr int ZOO_EXPIRED_SESSION_STATE = -112;
constexpr int ZOO_AUTH_FAILED_STATE = -113;

class ZooKeeperClient {
public:
    using WatchCallback = std::function<void(int type, int state, const char* path)>;

    explicit ZooKeeperClient(const std::string& hosts, int recv_timeout = 10000);
    ~ZooKeeperClient();

    bool connect();
    void disconnect();
    bool is_connected() const;
    int get_state() const;
    std::string get_current_server() const;

    int create_node(const std::string& path, const std::string& data, bool ephemeral = false, bool sequence = false);
    int delete_node(const std::string& path, int version = -1);
    bool node_exists(const std::string& path);
    std::string get_node_data(const std::string& path, bool watch = false);
    int set_node_data(const std::string& path, const std::string& data, int version = -1);
    std::vector<std::string> get_children(const std::string& path, bool watch = false);

    void set_watch_callback(WatchCallback cb);

private:
    void invoke_watch(int type, int state, const std::string& path);

    std::string hosts_;
    int recv_timeout_;
    WatchCallback watch_callback_;
    mutable std::mutex mutex_;
    bool connected_;
};
