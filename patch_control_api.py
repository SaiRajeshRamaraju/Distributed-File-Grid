import re

with open('src/head_server/control_api.hpp', 'r') as f:
    content = f.read()

content = content.replace(
    'int port = extract_json_int(body, "port");',
    'int port = extract_json_int(body, "port");\n        int public_port = extract_json_int(body, "public_port");'
)

content = content.replace(
    'int id = registry_.add_server(host, port);',
    'int id = registry_.add_server(host, port, public_port);'
)

with open('src/head_server/control_api.hpp', 'w') as f:
    f.write(content)
