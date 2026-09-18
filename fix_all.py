import re

# 1. Fix client.cpp includes
with open('src/client/client.cpp', 'r') as f:
    content = f.read()
if '<mutex>' not in content:
    content = '#include <mutex>\n#include <thread>\n#include <map>\n' + content
with open('src/client/client.cpp', 'w') as f:
    f.write(content)


# 2. Fix chunk_service.cpp public_port
with open('src/cluster_server/chunk_service.cpp', 'r') as f:
    content = f.read()

# Let's see if public_port is there
if 'int public_port;' not in content:
    content = content.replace(
        '  int port;\n  bool running = false;',
        '  int port;\n  int public_port;\n  bool running = false;'
    )

with open('src/cluster_server/chunk_service.cpp', 'w') as f:
    f.write(content)


# 3. Fix head_server.cpp forward declaration
with open('src/head_server/head_server.cpp', 'r') as f:
    content = f.read()

forward_decl = "\nextern std::unique_ptr<dfg::ServerRegistry> g_cluster_registry;\n"
if 'extern std::unique_ptr<dfg::ServerRegistry> g_cluster_registry;' not in content:
    # insert before FileReconstructor
    idx = content.find('class FileReconstructor {')
    if idx != -1:
        content = content[:idx] + forward_decl + content[idx:]

with open('src/head_server/head_server.cpp', 'w') as f:
    f.write(content)
