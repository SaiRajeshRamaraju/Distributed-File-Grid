#include "zookeeper_client.hpp"

#include <algorithm>
#include <iomanip>
#include <map>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

namespace {

struct Node {
	std::string data;
	bool ephemeral = false;
	int version = 0;
	std::set<std::string> children;
};

std::unordered_map<std::string, Node>& store() {
	static std::unordered_map<std::string, Node> nodes = [] {
		std::unordered_map<std::string, Node> initial;
		initial["/"] = Node{};
		return initial;
	}();
	return nodes;
}

std::unordered_map<std::string, int>& sequence_counters() {
	static std::unordered_map<std::string, int> counters;
	return counters;
}

std::mutex& store_mutex() {
	static std::mutex m;
	return m;
}

std::string normalize_path(const std::string& raw) {
	if (raw.empty()) {
		return "/";
	}
	// If path isn't started with '/' this will add it.	
	std::string path = raw;
	if (path.front() != '/') {
		path.insert(path.begin(), '/');
	}
	while (path.size() > 1 && path.back() == '/') {
		path.pop_back();
	}
	return path;
}

std::string parent_path(const std::string& path) {
	if (path == "/") {
		return "";
	}
	auto pos = path.find_last_of('/');
	if (pos == 0) {
		return "/";
	}
	return path.substr(0, pos);
}

std::string child_name(const std::string& path) {
	if (path == "/") {
		return "/";
	}
	auto pos = path.find_last_of('/');
	return path.substr(pos + 1);
}

bool ensure_parent_exists(const std::string& parent) {
	if (parent.empty()) {
		return true;
	}
	auto& nodes = store();
	return nodes.find(parent) != nodes.end();
}

} // namespace

ZooKeeperClient::ZooKeeperClient(const std::string& hosts, int recv_timeout)
	: hosts_(hosts), recv_timeout_(recv_timeout), connected_(false) {}

ZooKeeperClient::~ZooKeeperClient() { disconnect(); }

bool ZooKeeperClient::connect() {
	bool notify = false;
	{
		std::lock_guard<std::mutex> lock(mutex_);
		if (connected_) {
			return true;
		}
		connected_ = true;
		notify = true;
	}
	if (notify) {
		invoke_watch(ZOO_CREATED_EVENT, ZOO_CONNECTED_STATE, hosts_);
	}
	return true;
}

void ZooKeeperClient::disconnect() {
	bool notify = false;
	{
		std::lock_guard<std::mutex> lock(mutex_);
		if (!connected_) {
			return;
		}
		connected_ = false;
		notify = true;
	}
	if (notify) {
		invoke_watch(ZOO_DELETED_EVENT, ZOO_EXPIRED_SESSION_STATE, hosts_);
	}
}

bool ZooKeeperClient::is_connected() const {
	std::lock_guard<std::mutex> lock(mutex_);
	return connected_;
}

int ZooKeeperClient::get_state() const {
	std::lock_guard<std::mutex> lock(mutex_);
	return connected_ ? ZOO_CONNECTED_STATE : ZOO_CONNECTING_STATE;
}

std::string ZooKeeperClient::get_current_server() const { return hosts_; }

int ZooKeeperClient::create_node(const std::string& path, const std::string& data, bool ephemeral, bool sequence) {
	if (!is_connected()) {
		return ZINVALIDSTATE;
	}

	std::string normalized = normalize_path(path);
	std::string created_path;

	{
		std::lock_guard<std::mutex> lock(store_mutex());
		auto& nodes = store();
		std::string actual_path = normalized;

		if (sequence) {
			int seq = ++sequence_counters()[normalized];
			std::ostringstream oss;
			oss << normalized << '_' << std::setw(10) << std::setfill('0') << seq;
			actual_path = oss.str();
		}

		if (nodes.find(actual_path) != nodes.end()) {
			return ZNODEEXISTS;
		}

		std::string parent = parent_path(actual_path);
		if (!parent.empty() && !ensure_parent_exists(parent)) {
			return ZNONODE;
		}

		Node node;
		node.data = data;
		node.ephemeral = ephemeral;
		nodes[actual_path] = std::move(node);

		if (!parent.empty()) {
			nodes[parent].children.insert(child_name(actual_path));
		}

		created_path = actual_path;
	}

	invoke_watch(ZOO_CREATED_EVENT, ZOO_CHANGED_EVENT, created_path);
	return ZOK;
}

int ZooKeeperClient::delete_node(const std::string& path, int /*version*/) {
	if (!is_connected()) {
		return ZINVALIDSTATE;
	}

	std::string normalized = normalize_path(path);
	{
		std::lock_guard<std::mutex> lock(store_mutex());
		auto& nodes = store();
		auto it = nodes.find(normalized);
		if (it == nodes.end()) {
			return ZNONODE;
		}
		if (!it->second.children.empty()) {
			return ZINVALIDSTATE;
		}

		std::string parent = parent_path(normalized);
		if (!parent.empty()) {
			nodes[parent].children.erase(child_name(normalized));
		}

		nodes.erase(it);
	}

	invoke_watch(ZOO_DELETED_EVENT, ZOO_CHANGED_EVENT, normalized);
	return ZOK;
}

bool ZooKeeperClient::node_exists(const std::string& path) {
	std::string normalized = normalize_path(path);
	std::lock_guard<std::mutex> lock(store_mutex());
	return store().find(normalized) != store().end();
}

std::string ZooKeeperClient::get_node_data(const std::string& path, bool /*watch*/) {
	std::string normalized = normalize_path(path);
	std::lock_guard<std::mutex> lock(store_mutex());
	auto& nodes = store();
	auto it = nodes.find(normalized);
	if (it == nodes.end()) {
		return {};
	}
	return it->second.data;
}

int ZooKeeperClient::set_node_data(const std::string& path, const std::string& data, int /*version*/) {
	if (!is_connected()) {
		return ZINVALIDSTATE;
	}

	std::string normalized = normalize_path(path);
	{
		std::lock_guard<std::mutex> lock(store_mutex());
		auto& nodes = store();
		auto it = nodes.find(normalized);
		if (it == nodes.end()) {
			return ZNONODE;
		}
		it->second.data = data;
		++it->second.version;
	}

	invoke_watch(ZOO_CHANGED_EVENT, ZOO_CHANGED_EVENT, normalized);
	return ZOK;
}

std::vector<std::string> ZooKeeperClient::get_children(const std::string& path, bool /*watch*/) {
	std::string normalized = normalize_path(path);
	std::lock_guard<std::mutex> lock(store_mutex());
	auto& nodes = store();
	auto it = nodes.find(normalized);
	if (it == nodes.end()) {
		return {};
	}

	std::vector<std::string> children;
	children.reserve(it->second.children.size());
	for (const auto& child : it->second.children) {
		children.push_back(child);
	}
	return children;
}

void ZooKeeperClient::set_watch_callback(WatchCallback cb) {
	std::lock_guard<std::mutex> lock(mutex_);
	watch_callback_ = std::move(cb);
}

void ZooKeeperClient::invoke_watch(int type, int state, const std::string& path) {
	WatchCallback cb;
	{
		std::lock_guard<std::mutex> lock(mutex_);
		cb = watch_callback_;
	}
	if (cb) {
		cb(type, state, path.c_str());
	}
}

