#pragma once

#include "common.h"

class Client
{
public:
    Client(int client_fd_);
    void handleClient();
    void recvRequest(int32_t &request_corr_id, int16_t &request_api_ver);
    void sendResponse(int32_t &request_corr_id, int16_t &request_api_ver);
private:
    int client_fd;
    std::unordered_set<int16_t> supported_api_versions;
    std::unordered_map<int16_t, std::pair<int16_t, int16_t>> api_key_version_map;
    static constexpr size_t API_VERSIONS_SIZE = 3;
};
