import re

# chunk_service.cpp
with open('src/cluster_server/chunk_service.cpp', 'r') as f:
    chk = f.read()
chk = chk.replace('req.target_server_port() + 100;', '8180;')
with open('src/cluster_server/chunk_service.cpp', 'w') as f:
    f.write(chk)

# file_upload.cpp
with open('src/head_server/file_upload.cpp', 'r') as f:
    fu = f.read()
fu = fu.replace('port + 100;', '8180;')
with open('src/head_server/file_upload.cpp', 'w') as f:
    f.write(fu)

# head_server.cpp
with open('src/head_server/head_server.cpp', 'r') as f:
    hs = f.read()
hs = hs.replace('port + 100;', '8180;')
hs = hs.replace('port + 100,', '8180,')
with open('src/head_server/head_server.cpp', 'w') as f:
    f.write(hs)

