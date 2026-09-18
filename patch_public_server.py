import re

with open('src/cluster_server/chunk_service.cpp', 'r') as f:
    content = f.read()

# Add public_chunk_server method
public_server_code = """
  async_hb::task handle_public_connection(async_hb::Reactor &r, int cfd) {
    ConnectionTracker tracker(active_connections);
    try {
      char buf[256];
      ssize_t n = ::recv(cfd, buf, sizeof(buf) - 1, 0);
      if (n > 0) {
        buf[n] = 0;
        std::string req(buf);
        if (req.rfind("GET_CHUNK ", 0) == 0) {
          std::stringstream ss(req.substr(10));
          std::string chunk_id;
          ss >> chunk_id;

          auto fut = storage.async_retrieve_chunk(chunk_id);
          std::vector<char> data = co_await await_future(r, std::move(fut));
          
          if (!data.empty()) {
            std::string header = "CHUNK_SIZE " + std::to_string(data.size()) + "\\n";
            co_await async_hb::async_send_all(r, cfd, reinterpret_cast<const uint8_t*>(header.data()), header.size());
            co_await async_hb::async_send_all(r, cfd, reinterpret_cast<const uint8_t*>(data.data()), data.size());
          } else {
            std::string err = "ERROR: Chunk not found\\n";
            co_await async_hb::async_send_all(r, cfd, reinterpret_cast<const uint8_t*>(err.data()), err.size());
          }
        }
      }
    } catch (const std::exception &e) {
      std::cerr << "handle_public_connection error: " << e.what() << std::endl;
    }
    ::close(cfd);
    co_return;
  }

  async_hb::task public_chunk_server(async_hb::Reactor &reactor) {
    int lfd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (lfd < 0) {
      std::cerr << "Public listen socket failed\\n";
      co_return;
    }
    int yes = 1;
    setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(public_port);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if (::bind(lfd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
      std::cerr << "Bind failed for public chunk server\\n";
      ::close(lfd);
      co_return;
    }
    if (::listen(lfd, 128) < 0) {
      std::cerr << "Listen failed for public chunk server\\n";
      ::close(lfd);
      co_return;
    }
    if (async_hb::set_nonblock(lfd) < 0) {
      std::cerr << "Failed to set nonblocking public chunk server\\n";
      ::close(lfd);
      co_return;
    }

    std::cout << "Public chunk server started on port " << public_port << std::endl;

    while (running) {
      int cfd = -1;
      while (true) {
        sockaddr_in peer{};
        socklen_t len = sizeof(peer);
        cfd = ::accept4(lfd, reinterpret_cast<sockaddr *>(&peer), &len,
                        SOCK_CLOEXEC | SOCK_NONBLOCK);
        if (cfd >= 0)
          break;
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
          co_await reactor.wait_readable(lfd);
        } else {
          break;
        }
      }
      if (cfd >= 0) {
        reactor.spawn(handle_public_connection(reactor, cfd));
      }
    }
    ::close(lfd);
  }
"""

content = content.replace("public:\n  ClusterServerService", public_server_code + "\npublic:\n  ClusterServerService")

# Add reactor.spawn(public_chunk_server(reactor));
spawn_code = "reactor.spawn(chunk_server(reactor));\n\n    // Start public chunk server\n    reactor.spawn(public_chunk_server(reactor));"
content = content.replace("reactor.spawn(chunk_server(reactor));", spawn_code)

with open('src/cluster_server/chunk_service.cpp', 'w') as f:
    f.write(content)
