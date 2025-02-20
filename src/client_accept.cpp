#include "client_accept.h"

std::unordered_set<int> supported_api_versions{0, 1, 2, 3, 4};

void handleClient(int client_fd)
{
    int32_t request_msg_size, request_corr_id;
    int16_t request_api_key, request_api_ver;
    recv(client_fd, &request_msg_size, sizeof(request_msg_size), 0);
    recv(client_fd, &request_api_key, sizeof(request_api_key), 0);
    recv(client_fd, &request_api_ver, sizeof(request_api_ver), 0);
    recv(client_fd, &request_corr_id, sizeof(request_corr_id), 0);

    int32_t response_msg_size;
    int16_t error_code;
    response_msg_size = htobe32(0);
    send(client_fd, &response_msg_size, sizeof(response_msg_size), 0);
    send(client_fd, &request_corr_id, sizeof(request_corr_id), 0);
    request_api_ver = be32toh(request_api_ver);
    if (supported_api_versions.find(request_api_ver) != supported_api_versions.end())
    {
        error_code = 0;
    }
    else
    {
        error_code = 35;
    }
    error_code = htobe16(error_code);
    send(client_fd, &error_code, sizeof(error_code), 0);
    std::cout << "Send client response\n";

    while (true)
    {
    }

    close(client_fd);
}
