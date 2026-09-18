import re

# 1. Update file_transfer.proto
with open('src/proto/v1/file_transfer.proto', 'r') as f:
    proto = f.read()
proto = proto.replace('int32 target_server_port = 3;', 'int32 target_server_port = 3;\n  int32 target_transfer_port = 5;')
with open('src/proto/v1/file_transfer.proto', 'w') as f:
    f.write(proto)


# 2. Update server_registry.hpp
with open('src/common/include/dfg/server_registry.hpp', 'r') as f:
    reg = f.read()
reg = reg.replace('int public_port = -1;', 'int transfer_port = -1;\n    int public_port = -1;')
reg = reg.replace('int add_server(const std::string& host, int port, int public_port = -1) {',
                  'int add_server(const std::string& host, int port, int transfer_port = -1, int public_port = -1) {')
reg = reg.replace('entry.public_port = public_port;',
                  'entry.transfer_port = transfer_port;\n        entry.public_port = public_port;')
with open('src/common/include/dfg/server_registry.hpp', 'w') as f:
    f.write(reg)


# 3. Update control_api.hpp
with open('src/head_server/control_api.hpp', 'r') as f:
    api = f.read()
api = api.replace('int public_port = extract_json_int(body, "public_port");',
                  'int transfer_port = extract_json_int(body, "transfer_port");\n        int public_port = extract_json_int(body, "public_port");')
api = api.replace('int id = registry_.add_server(host, port, public_port);',
                  'int id = registry_.add_server(host, port, transfer_port, public_port);')
with open('src/head_server/control_api.hpp', 'w') as f:
    f.write(api)


# 4. Update cluster_server.cpp
with open('src/cluster_server/cluster_server.cpp', 'r') as f:
    cs = f.read()
# Replace default logic and variables
cs = cs.replace('int public_port = cfg.get_int("server.public_port", port + 200);',
                'int transfer_port = cfg.get_int("server.transfer_port", 8180);\n  int public_port = cfg.get_int("server.public_port", 8280);')

cs = cs.replace('else if (current_arg == "--public-port" && i + 1 < argc)',
                'else if (current_arg == "--transfer-port" && i + 1 < argc)\n      transfer_port = std::stoi(argv[++i]);\n    else if (current_arg == "--public-port" && i + 1 < argc)')
cs = cs.replace('std::cout << "  --public-port PORT Set public port" << std::endl;',
                'std::cout << "  --transfer-port PORT Set transfer port" << std::endl;\n      std::cout << "  --public-port PORT Set public port" << std::endl;')

cs = cs.replace('start_cluster_server(int server_id, const char *ip, int port, int public_port);',
                'start_cluster_server(int server_id, const char *ip, int port, int transfer_port, int public_port);')
cs = cs.replace('start_cluster_server(server_id, ip.c_str(), port, public_port);',
                'start_cluster_server(server_id, ip.c_str(), port, transfer_port, public_port);')

# self_register_with_head signature
cs = cs.replace('int server_id, const std::string &ip, int port, int public_port)',
                'int server_id, const std::string &ip, int port, int transfer_port, int public_port)')
cs = cs.replace('int server_id, const std::string &ip, int port, int public_port);',
                'int server_id, const std::string &ip, int port, int transfer_port, int public_port);')

# liveness monitor signature
cs = cs.replace('int server_id, const std::string& ip, int port, int public_port)',
                'int server_id, const std::string& ip, int port, int transfer_port, int public_port)')

# json construction
cs = cs.replace('<< "\\",\\"port\\":" << port << ",\\"public_port\\":" << public_port << "}";',
                '<< "\\",\\"port\\":" << port << ",\\"transfer_port\\":" << transfer_port << ",\\"public_port\\":" << public_port << "}";')

# calls
cs = cs.replace('self_register_with_head(head_host, head_control_port, server_id, ip, port, public_port)',
                'self_register_with_head(head_host, head_control_port, server_id, ip, port, transfer_port, public_port)')
cs = cs.replace('self_register_with_head(head_host, head_control_port, server_id,\n                                        ip, port, public_port)',
                'self_register_with_head(head_host, head_control_port, server_id,\n                                        ip, port, transfer_port, public_port)')
cs = cs.replace('monitor_thread(liveness_monitor, zk_hosts, head_host, head_control_port, server_id, ip, port, public_port)',
                'monitor_thread(liveness_monitor, zk_hosts, head_host, head_control_port, server_id, ip, port, transfer_port, public_port)')

cs = cs.replace('[head_host, head_control_port, server_id, ip, port, public_port]()',
                '[head_host, head_control_port, server_id, ip, port, transfer_port, public_port]()')

with open('src/cluster_server/cluster_server.cpp', 'w') as f:
    f.write(cs)


# 5. Update chunk_service.cpp
with open('src/cluster_server/chunk_service.cpp', 'r') as f:
    chk = f.read()
chk = chk.replace('int port;\n  int public_port;', 'int port;\n  int transfer_port;\n  int public_port;')
chk = chk.replace('ClusterServerService(int id, const std::string &ip, int p, int pp)',
                  'ClusterServerService(int id, const std::string &ip, int p, int tp, int pp)')
chk = chk.replace('server_ip(ip), port(p), public_port(pp) {}',
                  'server_ip(ip), port(p), transfer_port(tp), public_port(pp) {}')

chk = chk.replace('int start_cluster_server(int server_id, const char *ip, int port, int public_port) {',
                  'int start_cluster_server(int server_id, const char *ip, int port, int transfer_port, int public_port) {')
chk = chk.replace('std::make_unique<ClusterServerService>(server_id, ip, port, public_port);',
                  'std::make_unique<ClusterServerService>(server_id, ip, port, transfer_port, public_port);')

# replace + 100 
chk = chk.replace('int target_transfer_port = req.target_server_port() + 100;',
                  'int target_transfer_port = req.target_transfer_port();\n          if(target_transfer_port == 0) target_transfer_port = req.target_server_port() + 100;')

# chunk_server method binding port
chk = re.sub(r'int transfer_port = port \+ 100; // NOTE: Why not a static port id rather\n\s*// than 100\+ current server port\? Come out\n\s*// with a static port number',
             '', chk)
chk = chk.replace('int transfer_port = port + 100;', '')

with open('src/cluster_server/chunk_service.cpp', 'w') as f:
    f.write(chk)


# 6. Update head_server.cpp
with open('src/head_server/head_server.cpp', 'r') as f:
    hs = f.read()

# Helper macro string to inject port lookup logic
port_lookup = """
        int transfer_port = port + 100;
        if (g_cluster_registry) {
            for (const auto& s : g_cluster_registry->get_all()) {
                if (s.address() == location.server_ip || s.host == ip) {
                    if(s.transfer_port > 0) transfer_port = s.transfer_port;
                    break;
                }
            }
        }
"""

hs = re.sub(r'int transfer_port = port \+ 100;', port_lookup, hs)

# For send_dead_signal_to_cluster_server
hs = hs.replace('sock = dfg::net::connect_with_timeout(ip, port + 100, 2);',
                'int tp = port + 100;\n      if(g_cluster_registry) {\n        for (const auto& s : g_cluster_registry->get_all()) { if(s.address() == server_addr) { if(s.transfer_port>0) tp = s.transfer_port; break; } }\n      }\n      sock = dfg::net::connect_with_timeout(ip, tp, 2);')

with open('src/head_server/head_server.cpp', 'w') as f:
    f.write(hs)


# 7. Update file_upload.cpp
with open('src/head_server/file_upload.cpp', 'r') as f:
    fu = f.read()

fu = re.sub(r'int transfer_port = port \+ 100;', port_lookup, fu)

with open('src/head_server/file_upload.cpp', 'w') as f:
    f.write(fu)

