#include "client_accept.h"

std::unordered_set<int16_t> supported_api_versions{0, 1, 2, 3, 4};
std::unordered_map<int16_t, std::pair<int16_t, int16_t>> api_key_version_map;

void handleClient(int client_fd)
{
    int32_t request_msg_size, request_corr_id;
    int16_t request_api_key, request_api_ver;
    recv(client_fd, &request_msg_size, sizeof(request_msg_size), 0);
    recv(client_fd, &request_api_key, sizeof(request_api_key), 0);
    recv(client_fd, &request_api_ver, sizeof(request_api_ver), 0);
    recv(client_fd, &request_corr_id, sizeof(request_corr_id), 0);

    int32_t response_msg_size = sizeof(int32_t) + sizeof(int32_t) + sizeof(int16_t) + sizeof(int16_t) * 3;
    int16_t error_code;
    response_msg_size = htobe32(0);
    send(client_fd, &response_msg_size, sizeof(response_msg_size), 0);
    send(client_fd, &request_corr_id, sizeof(request_corr_id), 0);
    request_api_ver = be16toh(request_api_ver);
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
    for (auto &api_key_version : api_key_version_map)
    {
        int16_t api_key = htobe16(api_key_version.first);
        int16_t min_ver = htobe16(api_key_version.second.first);
        int16_t max_ver = htobe16(api_key_version.second.second);
        send(client_fd, &api_key, sizeof(api_key), 0);
        send(client_fd, &min_ver, sizeof(min_ver), 0);
        send(client_fd, &max_ver, sizeof(max_ver), 0);
    }
    std::cout << "Send client response\n";

    while (true)
    {
    }

    close(client_fd);
}
