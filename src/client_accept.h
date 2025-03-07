#pragma once

#include "common.h"
#include "kafka_utils.h"

class Client
{
public:
    Client(int client_fd_);
    void handleClient();

    std::unique_ptr<RequestHeader> recvRequestHeader();
    std::unique_ptr<RequestBody> recvRequestBody(int16_t api_key);
    ResponseMessage processMessage(RequestMessage request_message);
    void sendResponseHeader(std::unique_ptr<ResponseHeader> response_header);
    void sendResponseBody(std::unique_ptr<ResponseBody> response_body);

private:
    int client_fd;
};
