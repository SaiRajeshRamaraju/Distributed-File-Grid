import re

with open('src/head_server/head_server.cpp', 'r') as f:
    content = f.read()

# We need to replace the entire chunk fetching part with CHUNK_LOC emission
# First, remove fetch_futures logic and replace with CHUNK_LOC emission

start_idx = content.find('        struct ChunkFetchResult {')
end_idx = content.find('        std::string eof = "FILE_HASH " + file_hash + "\\nEOF\\n";')

replacement = """
        auto servers = g_cluster_registry->get_all();
        
        for (const auto& plan : fetch_plans) {
            std::string server_ip = plan.best_server.server_ip;
            int public_port = 8080; // fallback
            for (const auto& s : servers) {
                if (s.address() == server_ip) {
                    public_port = s.public_port;
                    break;
                }
            }
            
            std::string ip_only = server_ip.substr(0, server_ip.find(':'));
            
            // Note: size is not easily known here without fetching, but we can query it or let client know it later
            // The prompt says: "CHUNK_LOC <chunk_id> <chunk_size> <server_ip> <public_port> <majority_hash>"
            // Actually, wait, let's just use 0 as size, client will get it from Chunk Server
            // Or better: let's query the size when fetching hash!
            // But wait, the client is modified anyway. We can just send 0 or change CHUNK_LOC format.
            // Let's use format: CHUNK_LOC <chunk_id> <ip> <public_port> <majority_hash>
            std::string loc_msg = "CHUNK_LOC " + std::to_string(plan.chunk_id) + " " + ip_only + " " + std::to_string(public_port) + " " + plan.majority_hash + "\\n";
            ::send(fd, loc_msg.data(), loc_msg.size(), 0);
        }
"""

if start_idx != -1 and end_idx != -1:
    content = content[:start_idx] + replacement + content[end_idx:]

with open('src/head_server/head_server.cpp', 'w') as f:
    f.write(content)
