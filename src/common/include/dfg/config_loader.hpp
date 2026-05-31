#pragma once

#include <fstream>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <vector>
#include <algorithm>
#include <cstdlib>

// ─────────────────── Minimal JSON Config Loader ───────────────────
// Parses the flat key=value pairs from our JSON config files without
// pulling in a full JSON library.  Supports nested objects via dotted
// key paths, e.g. "server.port" → "9669".
//
// This is intentionally simple – production deployments can override
// any value with an environment variable using the DFG_ prefix:
//   DFG_SERVER_PORT=8000  → overrides "server.port" config key.

class ConfigLoader {
public:
    /// Load and parse a JSON configuration file.
    /// Returns false if the file cannot be opened.
    bool load(const std::string& filepath) {
        std::ifstream in(filepath);
        if (!in.is_open()) {
            return false;
        }

        std::string content((std::istreambuf_iterator<char>(in)),
                             std::istreambuf_iterator<char>());
        in.close();

        // Validate that the file has actual JSON content (not empty / whitespace-only)
        std::string trimmed = trim(content);
        if (trimmed.empty() || trimmed[0] != '{') {
            std::cerr << "\033[33m[WARNING]\033[0m ConfigLoader: File '" << filepath
                      << "' does not contain valid JSON (empty or malformed)" << std::endl;
            return false;
        }

        parse_json(content, "");

        // Verify we actually parsed something
        if (values_.empty()) {
            std::cerr << "\033[33m[WARNING]\033[0m ConfigLoader: File '" << filepath
                      << "' was parsed but yielded zero config entries" << std::endl;
            return false;
        }

        filepath_ = filepath;
        loaded_ = true;
        return true;
    }

    /// Get a string value.  Returns `default_val` if key is absent.
    std::string get_string(const std::string& key, const std::string& default_val = "") const {
        // Environment variable override: DFG_SERVER_PORT for "server.port"
        std::string env_key = "DFG_" + to_env_name(key);
        const char* env = std::getenv(env_key.c_str());
        if (env && *env) return std::string(env);

        auto it = values_.find(key);
        if (it != values_.end()) return it->second;
        return default_val;
    }

    /// Get an integer value.
    int get_int(const std::string& key, int default_val = 0) const {
        std::string v = get_string(key, "");
        if (v.empty()) return default_val;
        try { return std::stoi(v); } catch (...) { return default_val; }
    }

    /// Get a long long value.
    long long get_long(const std::string& key, long long default_val = 0) const {
        std::string v = get_string(key, "");
        if (v.empty()) return default_val;
        try { return std::stoll(v); } catch (...) { return default_val; }
    }

    /// Get a double value.
    double get_double(const std::string& key, double default_val = 0.0) const {
        std::string v = get_string(key, "");
        if (v.empty()) return default_val;
        try { return std::stod(v); } catch (...) { return default_val; }
    }

    /// Get a boolean value.
    bool get_bool(const std::string& key, bool default_val = false) const {
        std::string v = get_string(key, "");
        if (v.empty()) return default_val;
        return v == "true" || v == "1" || v == "yes";
    }

    /// Get the list of cluster server entries (id, host, port).
    struct ClusterServerEntry {
        int id;
        std::string host;
        int port;
    };

    std::vector<ClusterServerEntry> get_cluster_servers() const {
        std::vector<ClusterServerEntry> servers;
        // Probe for cluster_servers[0], cluster_servers[1], …
        for (int i = 0; i < 100; ++i) {
            std::string prefix = "cluster_servers." + std::to_string(i);
            std::string host = get_string(prefix + ".host", "");
            if (host.empty()) break;
            ClusterServerEntry entry;
            entry.id = get_int(prefix + ".id", i + 1);
            entry.host = host;
            entry.port = get_int(prefix + ".port", 8080 + i);
            servers.push_back(entry);
        }
        return servers;
    }

    /// Debug: dump all parsed key-value pairs.
    void dump() const {
        std::cout << "=== Config loaded from " << filepath_ << " ===" << std::endl;
        for (const auto& [k, v] : values_) {
            std::cout << "  " << k << " = " << v << std::endl;
        }
    }

    bool is_loaded() const { return loaded_; }

    /// Get the filepath that was successfully loaded (empty if none).
    const std::string& filepath() const { return filepath_; }

    /// Print a startup warning block when config was not loaded.
    /// Callers pass a service name and a list of {key, default_value} pairs
    /// so operators can see exactly which defaults are in effect.
    struct DefaultEntry {
        std::string key;
        std::string default_value;
    };

    void print_config_warnings(const std::string& service_name,
                               const std::vector<std::string>& search_paths,
                               const std::vector<DefaultEntry>& defaults) const {
        if (loaded_) return;  // Config loaded fine, nothing to warn about.

        std::cerr << std::endl;
        std::cerr << "\033[33m╔══════════════════════════════════════════════════════════════════╗\033[0m" << std::endl;
        std::cerr << "\033[33m║  ⚠  CONFIG NOT LOADED — " << service_name << std::endl;
        std::cerr << "\033[33m╚══════════════════════════════════════════════════════════════════╝\033[0m" << std::endl;
        std::cerr << "\033[33m  None of the following config files could be loaded:\033[0m" << std::endl;
        for (const auto& path : search_paths) {
            std::cerr << "\033[33m    • " << path << "\033[0m" << std::endl;
        }
        std::cerr << std::endl;
        std::cerr << "\033[33m  Using default values:\033[0m" << std::endl;
        for (const auto& d : defaults) {
            std::cerr << "\033[33m    " << d.key << " = " << d.default_value << "\033[0m" << std::endl;
        }
        std::cerr << std::endl;
        std::cerr << "\033[33m  To fix: place a valid JSON config file at one of the paths above,\033[0m" << std::endl;
        std::cerr << "\033[33m  or set individual values via DFG_<KEY> environment variables.\033[0m" << std::endl;
        std::cerr << std::endl;
    }

private:
    std::map<std::string, std::string> values_;
    std::string filepath_;
    bool loaded_ = false;

    /// Convert "server.port" → "SERVER_PORT" for env-var lookup.
    static std::string to_env_name(const std::string& key) {
        std::string result;
        result.reserve(key.size());
        for (char c : key) {
            if (c == '.') result += '_';
            else result += static_cast<char>(toupper(static_cast<unsigned char>(c)));
        }
        return result;
    }

    /// Strip leading/trailing whitespace.
    static std::string trim(const std::string& s) {
        size_t start = s.find_first_not_of(" \t\r\n");
        if (start == std::string::npos) return "";
        size_t end = s.find_last_not_of(" \t\r\n");
        return s.substr(start, end - start + 1);
    }

    /// Remove surrounding quotes if present.
    static std::string unquote(const std::string& s) {
        if (s.size() >= 2 && s.front() == '"' && s.back() == '"')
            return s.substr(1, s.size() - 2);
        return s;
    }

    /// Recursive descent parser for simple JSON.
    /// Stores results with dotted key prefixes.
    void parse_json(const std::string& json, const std::string& prefix) {
        size_t pos = 0;
        skip_ws(json, pos);
        if (pos >= json.size()) return;

        char c = json[pos];
        if (c == '{') {
            parse_object(json, pos, prefix);
        } else if (c == '[') {
            parse_array(json, pos, prefix);
        }
    }
    // Skip all whitespaces , new lines, tab and /r and return the position 
    // of the first valid character
    void skip_ws(const std::string& s, size_t& pos) const {
        while (pos < s.size() && (s[pos] == ' ' || s[pos] == '\t' || 
               s[pos] == '\r' || s[pos] == '\n'))
            ++pos;
    }

    std::string parse_string_token(const std::string& s, size_t& pos) const {
        if (pos >= s.size() || s[pos] != '"') return "";
        ++pos; // skip opening quote
        std::string result;
        while (pos < s.size() && s[pos] != '"') {
            if (s[pos] == '\\' && pos + 1 < s.size()) {
                ++pos;
                result += s[pos];
            } else {
                result += s[pos];
            }
            ++pos;
        }
        if (pos < s.size()) ++pos; // skip closing quote
        return result;
    }

    std::string parse_value_token(const std::string& s, size_t& pos) const {
        std::string result;
        while (pos < s.size() && s[pos] != ',' && s[pos] != '}' && 
               s[pos] != ']' && s[pos] != '\n' && s[pos] != '\r') {
            result += s[pos];
            ++pos;
        }
        return trim(result);
    }

    void parse_object(const std::string& s, size_t& pos, const std::string& prefix) {
        if (pos >= s.size() || s[pos] != '{') return;
        ++pos; // skip '{'

        while (pos < s.size()) {
            skip_ws(s, pos);
            if (pos >= s.size()) break;
            if (s[pos] == '}') { ++pos; return; }
            if (s[pos] == ',') { ++pos; continue; }

            // Expect key string
            std::string key = parse_string_token(s, pos);
            if (key.empty()) { ++pos; continue; }

            skip_ws(s, pos);
            if (pos < s.size() && s[pos] == ':') ++pos; // skip ':'
            skip_ws(s, pos);

            std::string full_key = prefix.empty() ? key : prefix + "." + key;

            if (pos >= s.size()) break;
            if (s[pos] == '{') {
                parse_object(s, pos, full_key);
            } else if (s[pos] == '[') {
                parse_array(s, pos, full_key);
            } else if (s[pos] == '"') {
                std::string val = parse_string_token(s, pos);
                values_[full_key] = val;
            } else {
                // Number, boolean, null
                std::string val = parse_value_token(s, pos);
                // Strip inline comments like "// comment"
                size_t comment_pos = val.find("//");
                if (comment_pos != std::string::npos)
                    val = trim(val.substr(0, comment_pos));
                values_[full_key] = val;
            }
        }
    }

    void parse_array(const std::string& s, size_t& pos, const std::string& prefix) {
        if (pos >= s.size() || s[pos] != '[') return;
        ++pos; // skip '['

        int index = 0;
        while (pos < s.size()) {
            skip_ws(s, pos);
            if (pos >= s.size()) break;
            if (s[pos] == ']') { ++pos; return; }
            if (s[pos] == ',') { ++pos; continue; }

            std::string elem_key = prefix + "." + std::to_string(index);

            if (s[pos] == '{') {
                parse_object(s, pos, elem_key);
            } else if (s[pos] == '[') {
                parse_array(s, pos, elem_key);
            } else if (s[pos] == '"') {
                std::string val = parse_string_token(s, pos);
                values_[elem_key] = val;
            } else {
                std::string val = parse_value_token(s, pos);
                values_[elem_key] = val;
            }
            ++index;
        }
    }
};

// ─────────────────── Global Config Singletons ───────────────────

/// Search paths used by each service config loader (kept visible so
/// callers can reference them when printing warnings).
namespace config_paths {
    inline const std::vector<std::string> head_server = {
        "config/head_server.json",
        "../config/head_server.json",
        "../../config/head_server.json",
        "/etc/dfg/head_server.json"
    };
    inline const std::vector<std::string> health_checker = {
        "config/health_checker.json",
        "../config/health_checker.json",
        "../../config/health_checker.json",
        "/etc/dfg/health_checker.json"
    };
    inline const std::vector<std::string> cluster_server = {
        "config/cluster_server.json",
        "../config/cluster_server.json",
        "../../config/cluster_server.json",
        "/etc/dfg/cluster_server.json"
    };
    inline const std::vector<std::string> zookeeper = {
        "config/zookeeper.json",
        "../config/zookeeper.json",
        "../../config/zookeeper.json",
        "/etc/dfg/zookeeper.json"
    };
}

inline ConfigLoader& head_server_config() {
    static ConfigLoader cfg;
    static bool tried = false;
    if (!tried) {
        tried = true;
        for (const auto& path : config_paths::head_server) {
            if (cfg.load(path)) break;
        }
        cfg.print_config_warnings("Head Server", config_paths::head_server, {
            {"server.host",                "0.0.0.0"},
            {"server.port",                "9669"},
            {"server.max_connections",     "1000"},
            {"server.timeout_seconds",     "30"},
            {"storage.chunk_size",         "67108864 (64 MB)"},
            {"storage.replication_factor", "3"},
            {"redis.host",                 "127.0.0.1"},
            {"redis.port",                 "6379"},
            {"logging.level",             "INFO"},
        });
    }
    return cfg;
}

inline ConfigLoader& health_checker_config() {
    static ConfigLoader cfg;
    static bool tried = false;
    if (!tried) {
        tried = true;
        for (const auto& path : config_paths::health_checker) {
            if (cfg.load(path)) break;
        }
        cfg.print_config_warnings("Health Checker", config_paths::health_checker, {
            {"server.host",                       "0.0.0.0"},
            {"server.port",                       "9000"},
            {"monitoring.heartbeat_interval",     "30"},
            {"monitoring.heartbeat_timeout",      "60"},
            {"monitoring.max_missed_heartbeats",  "3"},
            {"monitoring.health_check_interval",  "30"},
            {"failover.enable_auto_failover",     "true"},
            {"logging.level",                     "INFO"},
        });
    }
    return cfg;
}

inline ConfigLoader& cluster_server_config() {
    static ConfigLoader cfg;
    static bool tried = false;
    if (!tried) {
        tried = true;
        for (const auto& path : config_paths::cluster_server) {
            if (cfg.load(path)) break;
        }
        cfg.print_config_warnings("Cluster Server", config_paths::cluster_server, {
            {"server.host",                "0.0.0.0"},
            {"server.port",                "8080"},
            {"server.max_connections",     "500"},
            {"storage.chunk_dir",          "/data/chunks"},
            {"storage.max_storage_gb",     "100"},
            {"heartbeat.target_host",      "127.0.0.1"},
            {"heartbeat.target_port",      "9000"},
            {"heartbeat.interval_seconds", "30"},
            {"head_server.host",           "127.0.0.1"},
            {"head_server.port",           "9669"},
            {"logging.level",              "INFO"},
        });
    }
    return cfg;
}

inline ConfigLoader& zookeeper_config() {
    static ConfigLoader cfg;
    static bool tried = false;
    if (!tried) {
        tried = true;
        for (const auto& path : config_paths::zookeeper) {
            if (cfg.load(path)) break;
        }
        cfg.print_config_warnings("ZooKeeper Monitor", config_paths::zookeeper, {
            {"zookeeper.hosts",                  "localhost:2181"},
            {"zookeeper.session_timeout_ms",     "10000"},
            {"zookeeper.connection_timeout_ms",  "5000"},
            {"monitor.interval_seconds",         "10"},
            {"monitor.heartbeat_timeout_seconds","30"},
            {"logging.level",                    "INFO"},
        });
    }
    return cfg;
}
