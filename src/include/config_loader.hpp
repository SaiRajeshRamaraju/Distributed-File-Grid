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
            std::cerr << "ConfigLoader: Cannot open " << filepath << std::endl;
            return false;
        }

        std::string content((std::istreambuf_iterator<char>(in)),
                             std::istreambuf_iterator<char>());
        in.close();

        parse_json(content, "");
        filepath_ = filepath;
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

    bool is_loaded() const { return !filepath_.empty(); }

private:
    std::map<std::string, std::string> values_;
    std::string filepath_;

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

inline ConfigLoader& head_server_config() {
    static ConfigLoader cfg;
    static bool loaded = false;
    if (!loaded) {
        loaded = true;
        // Try multiple paths
        if (!cfg.load("config/head_server_config.json"))
            if (!cfg.load("../config/head_server_config.json"))
                cfg.load("/etc/dfg/head_server_config.json");
    }
    return cfg;
}

inline ConfigLoader& health_checker_config() {
    static ConfigLoader cfg;
    static bool loaded = false;
    if (!loaded) {
        loaded = true;
        if (!cfg.load("config/health_checker_config.json"))
            if (!cfg.load("../config/health_checker_config.json"))
                cfg.load("/etc/dfg/health_checker_config.json");
    }
    return cfg;
}
