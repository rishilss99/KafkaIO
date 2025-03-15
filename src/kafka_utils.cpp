#include "kafka_utils.h"
#include "log_parsing.h"

static void recvNullableString(int client_fd, int8_t &len, std::vector<char> &str)
{
    recv(client_fd, &len, sizeof(len), 0);
    str.resize(len);
    recv(client_fd, str.data(), len, 0);
}

static void recvNullableString(int client_fd, int16_t &len, std::vector<char> &str)
{
    recv(client_fd, &len, sizeof(len), 0);
    convertBE16toH(len);
    str.resize(len);
    recv(client_fd, str.data(), len, 0);
}

static void recvCompactString(int client_fd, int8_t &len, std::vector<char> &str)
{
    recv(client_fd, &len, sizeof(len), 0);
    str.resize(len - 1);
    recv(client_fd, str.data(), len - 1, 0);
}

static void recvCompactString(int client_fd, int16_t &len, std::vector<char> &str)
{
    recv(client_fd, &len, sizeof(len), 0);
    convertBE16toH(len);
    str.resize(len - 1);
    recv(client_fd, str.data(), len - 1, 0);
}

static void sendNullableString(int client_fd, int8_t &len, std::vector<char> &str)
{
    send(client_fd, &len, sizeof(len), 0);
    send(client_fd, str.data(), len, 0);
}

static void sendNullableString(int client_fd, int16_t &len, std::vector<char> &str)
{
    send(client_fd, &len, sizeof(len), 0);
    convertBE16toH(len);
    send(client_fd, str.data(), len, 0);
}

static void sendCompactString(int client_fd, int8_t &len, std::vector<char> &str)
{
    send(client_fd, &len, sizeof(len), 0);
    send(client_fd, str.data(), len - 1, 0);
}

static void sendCompactString(int client_fd, int16_t &len, std::vector<char> &str)
{
    send(client_fd, &len, sizeof(len), 0);
    convertBE16toH(len);
    send(client_fd, str.data(), len - 1, 0);
}

void RequestHeaderV2::receive(int client_fd)
{
    recv(client_fd, &request_msg_size, sizeof(request_msg_size), 0);
    recv(client_fd, &request_api_key, sizeof(request_api_key), 0);
    recv(client_fd, &request_api_ver, sizeof(request_api_ver), 0);
    recv(client_fd, &request_corr_id, sizeof(request_corr_id), 0);
    recvNullableString(client_fd, client_id_len, client_id_contents);
    recv(client_fd, &tag_buffer, sizeof(tag_buffer), 0);

    convertBEToH();
}

int16_t RequestHeaderV2::getAPIKey()
{
    return request_api_key;
}

void RequestHeaderV2::convertBEToH()
{
    convertBE16toH(request_api_key, request_api_ver);
    convertBE32toH(request_msg_size, request_corr_id);
}

void APIVersionsRequestBodyV4::receive(int client_fd)
{
    recvCompactString(client_fd, client_id_len, client_id_contents);
    recvCompactString(client_fd, client_software_version_len, client_software_version_contents);
    recv(client_fd, &tag_buffer, sizeof(tag_buffer), 0);

    convertBEToH();
}

void APIVersionsRequestBodyV4::convertBEToH()
{
}

void DescribeTopicPartitionsRequestBodyV0::receive(int client_fd)
{
    recv(client_fd, &topics_array_len, sizeof(topics_array_len), 0);
    topics_array.resize(topics_array_len - 1);
    for (int i = 0; i < topics_array_len - 1; i++)
    {
        recvCompactString(client_fd, topics_array[i].topic_name_len, topics_array[i].topic_name);
        recv(client_fd, &topics_array[i].tag_buffer, sizeof(topics_array[i].tag_buffer), 0);
    }
    recv(client_fd, &response_part_limit, sizeof(response_part_limit), 0);
    recv(client_fd, &cursor, sizeof(cursor), 0);
    recv(client_fd, &tag_buffer, sizeof(tag_buffer), 0);

    convertBEToH();
}

void DescribeTopicPartitionsRequestBodyV0::convertBEToH()
{
    convertBE32toH(response_part_limit);
}

void ResponseHeaderV0::respond(int client_fd)
{
    convertHToBE();

    send(client_fd, &response_msg_size, sizeof(response_msg_size), 0);
    send(client_fd, &response_corr_id, sizeof(response_corr_id), 0);
}

void ResponseHeaderV0::convertHToBE()
{
    convertH32toBE(response_msg_size, response_corr_id);
}

void ResponseHeaderV1::respond(int client_fd)
{
    convertHToBE();

    send(client_fd, &response_msg_size, sizeof(response_msg_size), 0);
    send(client_fd, &response_corr_id, sizeof(response_corr_id), 0);
    send(client_fd, &tag_buffer, sizeof(tag_buffer), 0);
}

void ResponseHeaderV1::convertHToBE()
{
    convertH32toBE(response_msg_size, response_corr_id);
}

void APIVersionsResponseBodyV4::respond(int client_fd)
{
    convertHToBE();

    send(client_fd, &error_code, sizeof(error_code), 0);
    send(client_fd, &api_versions_array_len, sizeof(api_versions_array_len), 0);
    for (auto &api_versions_elem : api_versions_array)
    {
        send(client_fd, &api_versions_elem.api_key, sizeof(api_versions_elem.api_key), 0);
        send(client_fd, &api_versions_elem.api_min_ver, sizeof(api_versions_elem.api_min_ver), 0);
        send(client_fd, &api_versions_elem.api_max_ver, sizeof(api_versions_elem.api_max_ver), 0);
        send(client_fd, &api_versions_elem.tag_buffer, sizeof(api_versions_elem.tag_buffer), 0);
    }
    send(client_fd, &throttle_time, sizeof(throttle_time), 0);
    send(client_fd, &tag_buffer, sizeof(tag_buffer), 0);
}

void APIVersionsResponseBodyV4::convertHToBE()
{
    convertH16toBE(error_code);
    convertH32toBE(throttle_time);

    for (auto &api_versions_elem : api_versions_array)
    {
        convertH16toBE(api_versions_elem.api_key, api_versions_elem.api_min_ver, api_versions_elem.api_max_ver);
    }
}

void DescribeTopicPartitionsResponseBodyV0::respond(int client_fd)
{
    convertHToBE();

    send(client_fd, &throttle_time, sizeof(throttle_time), 0);
    send(client_fd, &topics_array_len, sizeof(topics_array_len), 0);
    for (auto &topics_elem : topics_array)
    {
        send(client_fd, &topics_elem.error_code, sizeof(topics_elem.error_code), 0);
        sendCompactString(client_fd, topics_elem.topic_name_len, topics_elem.topic_name);
        send(client_fd, topics_elem.topic_id.data(), topics_elem.topic_id.size(), 0);
        send(client_fd, &topics_elem.is_internal, sizeof(topics_elem.is_internal), 0
        send(client_fd, &topics_elem.partitions_array_len, sizeof(topics_elem.partitions_array_len), 0);
        for (auto &partitions_elem : topics_elem.partitions_array)
        {
            send(client_fd, &partitions_elem.error_code, sizeof(partitions_elem.error_code), 0);
            send(client_fd, &partitions_elem.partition_index, sizeof(partitions_elem.partition_index), 0);
            send(client_fd, &partitions_elem.leader_id, sizeof(partitions_elem.leader_id), 0);
            send(client_fd, &partitions_elem.leader_epoch, sizeof(partitions_elem.leader_epoch), 0);
            send(client_fd, &partitions_elem.replica_nodes_array_len, sizeof(partitions_elem.replica_nodes_array_len), 0);
            for (auto &replica_node : partitions_elem.replica_nodes_array)
            {
                send(client_fd, &replica_node, sizeof(replica_node), 0);
            }
            send(client_fd, &partitions_elem.isr_nodes_array_len, sizeof(partitions_elem.isr_nodes_array_len), 0);
            for (auto &isr_node : partitions_elem.isr_nodes_array)
            {
                send(client_fd, &isr_node, sizeof(isr_node), 0);
            }
            send(client_fd, &partitions_elem.elr_nodes_array_len, sizeof(partitions_elem.elr_nodes_array_len), 0);
            for (auto &elr_node : partitions_elem.elr_nodes_array)
            {
                send(client_fd, &elr_node, sizeof(elr_node), 0);
            }
            send(client_fd, &partitions_elem.last_known_elr_nodes_array_len, sizeof(partitions_elem.last_known_elr_nodes_array_len), 0);
            for (auto &last_known_elr_node : partitions_elem.last_known_elr_nodes_array)
            {
                send(client_fd, &last_known_elr_node, sizeof(last_known_elr_node), 0);
            }
            send(client_fd, &partitions_elem.offline_replica_nodes_array_len, sizeof(partitions_elem.offline_replica_nodes_array_len), 0);
            for (auto &offline_replica_node : partitions_elem.offline_replica_nodes_array)
            {
                send(client_fd, &offline_replica_node, sizeof(offline_replica_node), 0);
            }
            send(client_fd, &partitions_elem.tag_buffer, sizeof(partitions_elem.tag_buffer), 0);
        }
        send(client_fd, &topics_elem.is_internal, sizeof(topics_elem.is_internal), 0);
        send(client_fd, &topics_elem.topic_authorized_ops, sizeof(topics_elem.topic_authorized_ops), 0);
        send(client_fd, &topics_elem.tag_buffer, sizeof(topics_elem.tag_buffer), 0);
    }
    send(client_fd, &next_cursor, sizeof(next_cursor), 0);
    send(client_fd, &tag_buffer, sizeof(tag_buffer), 0);
}

void DescribeTopicPartitionsResponseBodyV0::convertHToBE()
{
    convertH32toBE(throttle_time);

    for (auto &topics_elem : topics_array)
    {
        convertH16toBE(topics_elem.error_code);
        convertH32toBE(topics_elem.topic_authorized_ops);

        for (auto &partitions_elem : topics_elem.partitions_array)
        {
            convertH16toBE(partitions_elem.error_code);
            convertH32toBE(partitions_elem.partition_index, partitions_elem.leader_id, partitions_elem.leader_epoch);

            for (auto &replica_node : partitions_elem.replica_nodes_array)
            {
                convertH32toBE(replica_node);
            }

            for (auto &isr_node : partitions_elem.isr_nodes_array)
            {
                convertH32toBE(isr_node);
            }

            for (auto &elr_node : partitions_elem.elr_nodes_array)
            {
                convertH32toBE(elr_node);
            }

            for (auto &last_known_elr_node : partitions_elem.last_known_elr_nodes_array)
            {
                convertH32toBE(last_known_elr_node);
            }

            for (auto &offline_replica_node : partitions_elem.offline_replica_nodes_array)
            {
                convertH32toBE(offline_replica_node);
            }
        }
    }
}

ResponseMessage processAPIVersions(const RequestHeaderV2 &request_header, const APIVersionsRequestBodyV4 &request_body)
{
    // Move these into a new processing module
    constexpr size_t API_VERSIONS_SIZE = 3;
    std::vector<int16_t> supported_api_versions = {0, 1, 2, 3, 4};
    std::vector<std::array<int16_t, API_VERSIONS_SIZE>> api_key_versions;
    api_key_versions.push_back({18, 0, 4}); // APIVersions
    api_key_versions.push_back({75, 0, 4}); // DescribeTopicPartitions

    // Response message

    auto response_header = std::make_unique<ResponseHeaderV0>();
    auto response_body = std::make_unique<APIVersionsResponseBodyV4>();

    int32_t response_size = 0;

    // Process bottom-up - Response Body -> Response Header

    // Response Body

    // Supported API versions is actually part of request header but we use here

    if (std::find(supported_api_versions.begin(), supported_api_versions.end(), request_header.request_api_ver) != supported_api_versions.end())
    {
        response_body->error_code = 0;
        response_size += sizeof(response_body->error_code);

        response_body->api_versions_array_len = api_key_versions.size() + 1;
        response_size += sizeof(response_body->api_versions_array_len);

        for (auto &api_versions_elem : api_key_versions)
        {
            APIVersionsResponseBodyV4::APIVersion api_version = {.api_key = api_versions_elem[0],
                                                                 .api_min_ver = api_versions_elem[1],
                                                                 .api_max_ver = api_versions_elem[2],
                                                                 .tag_buffer = 0};

            response_body->api_versions_array.push_back(api_version);
            response_size += api_version.size();
        }

        response_body->throttle_time = 0;
        response_size += sizeof(response_body->throttle_time);

        response_body->tag_buffer = 0;
        response_size += sizeof(response_body->tag_buffer);
    }
    else
    {
        response_body->error_code = 35;
        response_size += sizeof(response_body->error_code);
    }

    // Response Header

    response_header->response_corr_id = request_header.request_corr_id;
    response_size += sizeof(response_header->response_corr_id);

    response_header->response_msg_size = response_size;

    return {std::move(response_header), std::move(response_body)};
}

ResponseMessage processDescribeTopicPartitions(const RequestHeaderV2 &request_header, const DescribeTopicPartitionsRequestBodyV0 &request_body)
{
    // Response message

    auto response_header = std::make_unique<ResponseHeaderV1>();
    auto response_body = std::make_unique<DescribeTopicPartitionsResponseBodyV0>();

    int32_t response_size = 0;

    // Process bottom-up - Response Body -> Response Header

    // Response Body

    // Might consider using something like supported_api_versions later, not needed for current tests

    response_body->throttle_time = 0;
    response_size += sizeof(response_body->throttle_time);

    response_body->topics_array_len = request_body.topics_array_len;
    response_size += sizeof(response_body->topics_array_len);

    // This is where we use the LogParser for checking log files for the topics

    LogParser log_parser("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"); // Hard-coded filename is not good

    // KISS

    for (auto &topics_elem : request_body.topics_array)
    {
        auto topic = log_parser.extractTopicPartitionRecords(topics_elem.topic_name_len, topics_elem.topic_name);

        response_body->topics_array.push_back(topic);
        response_size += topic.size();
    }

    response_body->next_cursor = 0xFF;
    response_size += sizeof(response_body->next_cursor);

    response_body->tag_buffer = 0;
    response_size += sizeof(response_body->tag_buffer);

    // Response Header

    response_header->response_corr_id = request_header.request_corr_id;
    response_size += sizeof(response_header->response_corr_id);

    response_header->tag_buffer = 0;
    response_size += sizeof(response_header->tag_buffer);

    response_header->response_msg_size = response_size;

    return {std::move(response_header), std::move(response_body)};
}