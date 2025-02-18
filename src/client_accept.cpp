#include "client_accept.h"

void handleClient(int client_fd)
{
    int32_t request_msg_size, request_corr_id;
    int16_t request_api_key, request_api_ver;
    recv(client_fd, &request_msg_size, sizeof(request_msg_size), 0);
    recv(client_fd, &request_api_key, sizeof(request_api_key), 0);
    recv(client_fd, &request_api_ver, sizeof(request_api_ver), 0);
    recv(client_fd, &request_corr_id, sizeof(request_corr_id), 0);

    int32_t response_msg_size = htobe32(0);
    send(client_fd, &response_msg_size, sizeof(response_msg_size), 0);
    send(client_fd, &request_corr_id, sizeof(request_corr_id), 0);
    std::cout << "Send client response\n";

    while (true)
    {
    }

    close(client_fd);
}
