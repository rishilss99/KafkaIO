#include "client_accept.h"

Client::Client(int client_fd_) : client_fd(client_fd_)
{
    supported_api_versions.insert({0, 1, 2, 3, 4});
    api_key_version_map.insert({18, {0, 4}});   // APIVersions
    api_key_version_map.insert({75, {0, 4}});   // DescribeTopicPartitions
    handleClient();
}

void Client::recvRequest(int32_t &request_corr_id, int16_t &request_api_ver)
{
    int16_t request_api_key;
    int32_t request_msg_size;

    recv(client_fd, &request_msg_size, sizeof(request_msg_size), 0);
    recv(client_fd, &request_api_key, sizeof(request_api_key), 0);
    recv(client_fd, &request_api_ver, sizeof(request_api_ver), 0);
    recv(client_fd, &request_corr_id, sizeof(request_corr_id), 0);

    convertBE16toH(request_api_key, request_api_ver);
    convertBE32toH(request_msg_size, request_corr_id);

    size_t request_left_size = request_msg_size - sizeof(int16_t) // api_key
                               - sizeof(int16_t)                  // api_ver
                               - sizeof(int32_t);                 // correlation id

    if (request_left_size)
    {
        std::vector<char> request_left_msg(request_left_size);
        recv(client_fd, request_left_msg.data(), request_left_msg.size(), 0);
    }
}

void Client::sendResponse(int32_t &request_corr_id, int16_t &request_api_ver)
{
    int8_t array_len, tag_buffer;
    int16_t error_code;
    int32_t response_msg_size, throttle_time;
    std::vector<std::array<int16_t, API_VERSIONS_SIZE>> api_versions_vec;

    response_msg_size = sizeof(int32_t) +                     // correlation id
                        sizeof(int16_t) +                     // error code
                        sizeof(int8_t) +                      // array length
                        sizeof(int16_t) * API_VERSIONS_SIZE + // api_key, min_ver, max_ver
                        sizeof(int8_t) +                      // tag buffer
                        sizeof(int32_t) +                     // throttle time
                        sizeof(int8_t);                       // tag buffer

    if (supported_api_versions.find(request_api_ver) != supported_api_versions.end())
    {
        error_code = 0;
    }
    else
    {
        error_code = 35;
    }

    array_len = api_key_version_map.size() + 1;

    for (auto &api_key_version : api_key_version_map)
    {
        api_versions_vec.push_back({api_key_version.first, api_key_version.second.first, api_key_version.second.second});
    }

    throttle_time = 0;
    tag_buffer = 0;

    convertH16toBE(request_api_ver, error_code);
    for (auto &api_versions_elem : api_versions_vec)
    {
        for (auto &val : api_versions_elem)
        {
            convertH16toBE(val);
        }
    }
    convertH32toBE(response_msg_size, request_corr_id, throttle_time);

    send(client_fd, &response_msg_size, sizeof(response_msg_size), 0);
    send(client_fd, &request_corr_id, sizeof(request_corr_id), 0);
    send(client_fd, &error_code, sizeof(error_code), 0);
    send(client_fd, &array_len, sizeof(array_len), 0);
    for (auto &api_versions_elem : api_versions_vec)
    {
        for (auto &val : api_versions_elem)
        {
            send(client_fd, &val, sizeof(val), 0);
        }
        send(client_fd, &tag_buffer, sizeof(tag_buffer), 0);
    }
    send(client_fd, &throttle_time, sizeof(throttle_time), 0);
    send(client_fd, &tag_buffer, sizeof(tag_buffer), 0);
    std::cout << "Sent client response\n";
}

void Client::handleClient()
{
    int32_t request_corr_id;
    int16_t request_api_ver;

    while (server_running.load())
    {
        recvRequest(request_corr_id, request_api_ver);
        sendResponse(request_corr_id, request_api_ver);
    }

    close(client_fd);
}
