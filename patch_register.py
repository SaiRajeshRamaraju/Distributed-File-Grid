import re

with open('src/cluster_server/cluster_server.cpp', 'r') as f:
    content = f.read()

content = content.replace(
    'static bool self_register_with_head(const std::string &head_host, int head_control_port, int server_id, const std::string &ip, int port);',
    'static bool self_register_with_head(const std::string &head_host, int head_control_port, int server_id, const std::string &ip, int port, int public_port);'
)

content = content.replace(
    'static void liveness_monitor(const std::string& zk_hosts, const std::string& head_host, int head_port, int server_id, const std::string& ip, int port) {',
    'static void liveness_monitor(const std::string& zk_hosts, const std::string& head_host, int head_port, int server_id, const std::string& ip, int port, int public_port) {'
)

content = content.replace(
    'bool head_ok = self_register_with_head(head_host, head_port, server_id, ip, port);',
    'bool head_ok = self_register_with_head(head_host, head_port, server_id, ip, port, public_port);'
)

content = content.replace(
    'static bool self_register_with_head(const std::string &head_host,\n                                    int head_control_port, int server_id,\n                                    const std::string &ip, int port) {',
    'static bool self_register_with_head(const std::string &head_host,\n                                    int head_control_port, int server_id,\n                                    const std::string &ip, int port, int public_port) {'
)

content = content.replace(
    'body << "{\\"server_id\\":" << server_id << ",\\"host\\":\\"" << ip\n       << "\\",\\"port\\":" << port << "}";',
    'body << "{\\"server_id\\":" << server_id << ",\\"host\\":\\"" << ip\n       << "\\",\\"port\\":" << port << ",\\"public_port\\":" << public_port << "}";'
)

content = content.replace(
    'self_register_with_head(head_host, head_control_port, server_id,\n                                        ip, port)',
    'self_register_with_head(head_host, head_control_port, server_id,\n                                        ip, port, public_port)'
)

content = content.replace(
    'std::thread monitor_thread(liveness_monitor, zk_hosts, head_host, head_control_port, server_id, ip, port);',
    'std::thread monitor_thread(liveness_monitor, zk_hosts, head_host, head_control_port, server_id, ip, port, public_port);'
)

with open('src/cluster_server/cluster_server.cpp', 'w') as f:
    f.write(content)
