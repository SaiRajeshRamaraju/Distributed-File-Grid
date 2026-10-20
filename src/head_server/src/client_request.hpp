#pragma once

// Takes ownership of the accepted socket and handles one client command.
void handle_client_request(int socket);
