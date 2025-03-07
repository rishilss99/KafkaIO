#include "client_accept.h"

Client::Client(int client_fd_) : client_fd(client_fd_)
{
    handleClient();
}

std::unique_ptr<RequestHeader> Client::recvRequestHeader()
{
    std::unique_ptr<RequestHeader> request_header = std::make_unique<RequestHeaderV2>();
    request_header->receive(client_fd);
    return request_header;
}

std::unique_ptr<RequestBody> Client::recvRequestBody(int16_t api_key)
{
    std::unique_ptr<RequestBody> request_body = nullptr;

    switch (api_key)
    {
    case 18: // APIVersions
        request_body = std::make_unique<APIVersionsRequestBodyV4>();
        break;

    case 75: // DescribeTopicPartitions
        request_body = std::make_unique<DescribeTopicPartitionsRequestBodyV0>();
        break;

    default:
        assert(true); // No handling of unknown API keys
        break;
    }
    request_body->receive(client_fd);
    return request_body;
}

ResponseMessage Client::processMessage(RequestMessage request_message)
{
    auto [request_header, request_body] = std::move(request_message);
    ResponseMessage response_message = {nullptr, nullptr};

    switch (request_header->getAPIKey())
    {
    case 18: // APIVersions
        response_message = processAPIVersions(dynamic_cast<const RequestHeaderV2 &>(*request_header), dynamic_cast<const APIVersionsRequestBodyV4 &>(*request_body));
        break;

    case 75: // DescribeTopicPartitions
        response_message = processDescribeTopicPartitions(dynamic_cast<const RequestHeaderV2 &>(*request_header), dynamic_cast<const DescribeTopicPartitionsRequestBodyV0 &>(*request_body));
        break;

    default:
        assert(true); // No handling of unknown API keys
        break;
    }

    return response_message;
}

void Client::sendResponseHeader(std::unique_ptr<ResponseHeader> response_header)
{
    response_header->respond(client_fd);
}

void Client::sendResponseBody(std::unique_ptr<ResponseBody> response_body)
{
    response_body->respond(client_fd);
}

void Client::handleClient()
{
    int32_t request_corr_id;
    int16_t request_api_ver;

    while (server_running.load())
    {
        auto request_header = recvRequestHeader();
        auto request_body = recvRequestBody(request_header->getAPIKey());
        auto [response_header, response_body] = processMessage({std::move(request_header), std::move(request_body)});
        sendResponseHeader(std::move(response_header));
        sendResponseBody(std::move(response_body));
    }

    close(client_fd);
}
