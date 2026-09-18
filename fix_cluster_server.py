import re

with open('src/cluster_server/cluster_server.cpp', 'r') as f:
    cs = f.read()

# Fix self_register_with_head signature
cs = re.sub(
    r'static bool self_register_with_head\((.*?),\s*int public_port\) \{',
    r'static bool self_register_with_head(\1, int transfer_port, int public_port) {',
    cs,
    flags=re.DOTALL
)

# Fix self_register_with_head call in liveness_monitor
cs = cs.replace(
    'self_register_with_head(head_host, head_port, server_id, ip, port, public_port);',
    'self_register_with_head(head_host, head_port, server_id, ip, port, transfer_port, public_port);'
)

# Also let's make sure liveness_monitor signature is correct. Wait, in my previous python script I did:
# cs = cs.replace('int server_id, const std::string& ip, int port, int public_port)',
#                 'int server_id, const std::string& ip, int port, int transfer_port, int public_port)')
# Did that work? Let's check liveness_monitor signature in cs.

with open('src/cluster_server/cluster_server.cpp', 'w') as f:
    f.write(cs)
