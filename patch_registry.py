import re

with open('src/common/include/dfg/server_registry.hpp', 'r') as f:
    content = f.read()

content = content.replace(
    'int port;\n    std::string status = "active";',
    'int port;\n    int public_port = -1;\n    std::string status = "active";'
)

content = content.replace(
    'int add_server(const std::string& host, int port) {',
    'int add_server(const std::string& host, int port, int public_port = -1) {'
)

content = content.replace(
    'entry.port = port;\n        entry.registered_at = std::chrono::steady_clock::now();',
    'entry.port = port;\n        entry.public_port = public_port;\n        entry.registered_at = std::chrono::steady_clock::now();'
)

with open('src/common/include/dfg/server_registry.hpp', 'w') as f:
    f.write(content)
