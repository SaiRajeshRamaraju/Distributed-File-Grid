import re

with open('src/cluster_server/chunk_service.cpp', 'r') as f:
    content = f.read()

replacement = """
        if (req.rfind("GET_CHUNK ", 0) == 0) {
          std::stringstream ss(req.substr(10));
          std::string chunk_id, file_name;
          ss >> chunk_id >> file_name;
          
          std::string unique_chunk_id = file_name + "_chunk_" + chunk_id;

          auto fut = storage.async_retrieve_chunk(unique_chunk_id);
"""

# replace the block
content = re.sub(r'if \(req.rfind\("GET_CHUNK ", 0\) == 0\) \{.*?auto fut = storage.async_retrieve_chunk\(chunk_id\);', replacement, content, flags=re.DOTALL)

with open('src/cluster_server/chunk_service.cpp', 'w') as f:
    f.write(content)
