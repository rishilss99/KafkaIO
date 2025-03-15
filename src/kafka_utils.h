#pragma once

#include "common.h"

// Request Header classes
class RequestHeader;
class RequestHeaderV2;

// Request Body classes
class RequestBody;
class APIVersionsRequestBodyV4;
class DescribeTopicPartitionsRequestBodyV0;

// Response Header classes
class ResponseHeader;
class ResponseHeaderV0;
class ResponseHeaderV1;

// Response Body classes
class ResponseBody;
class APIVersionsResponseBodyV4;
class DescribeTopicPartitionsResponseBodyV0;

using RequestMessage = std::pair<std::unique_ptr<RequestHeader>, std::unique_ptr<RequestBody>>;
using ResponseMessage = std::pair<std::unique_ptr<ResponseHeader>, std::unique_ptr<ResponseBody>>;

class RequestHeader
{
public:
    virtual ~RequestHeader() {}
    virtual void receive(int client_fd) = 0;
    virtual int16_t getAPIKey() = 0;

private:
    virtual void convertBEToH() = 0;
};

class RequestHeaderV2 : public RequestHeader
{
public:
    RequestHeaderV2() = default;
    void receive(int client_fd) override;
    int16_t getAPIKey() override;

private:
    void convertBEToH() override;

    int32_t request_msg_size;
    int16_t request_api_key;
    int16_t request_api_ver;
    int32_t request_corr_id;
    int16_t client_id_len;
    std::vector<char> client_id_contents; // Kafka Nullable string (N)
    int8_t tag_buffer;

    friend ResponseMessage processAPIVersions(const RequestHeaderV2 &request_header, const APIVersionsRequestBodyV4 &request_body);
    friend ResponseMessage processDescribeTopicPartitions(const RequestHeaderV2 &request_header, const DescribeTopicPartitionsRequestBodyV0 &request_body);
};

class RequestBody
{
public:
    virtual ~RequestBody() {}
    virtual void receive(int client_fd) = 0;

private:
    virtual void convertBEToH() = 0;
};

class APIVersionsRequestBodyV4 : public RequestBody
{
public:
    APIVersionsRequestBodyV4() = default;
    void receive(int client_fd) override;

private:
    void convertBEToH() override;

    int8_t client_id_len;
    std::vector<char> client_id_contents; // Kafka Compact string (N+1)
    int8_t client_software_version_len;
    std::vector<char> client_software_version_contents; // Kafka Compact string (N+1)
    int8_t tag_buffer;

    friend ResponseMessage processAPIVersions(const RequestHeaderV2 &request_header, const APIVersionsRequestBodyV4 &request_body);
};

class DescribeTopicPartitionsRequestBodyV0 : public RequestBody
{
public:
    DescribeTopicPartitionsRequestBodyV0() = default;
    void receive(int client_fd) override;

public:
    struct Topic
    {
        int8_t topic_name_len;
        std::vector<char> topic_name; // Kafka Compact string (N+1)
        int8_t tag_buffer;
    };

private:
    void convertBEToH() override;

    int8_t topics_array_len;
    std::vector<Topic> topics_array; // Kafka Compact arry (N+1)
    int32_t response_part_limit;
    int8_t cursor;
    int8_t tag_buffer;

    friend ResponseMessage processDescribeTopicPartitions(const RequestHeaderV2 &request_header, const DescribeTopicPartitionsRequestBodyV0 &request_body);
};

class ResponseHeader
{
public:
    virtual ~ResponseHeader() {}
    virtual void respond(int client_fd) = 0;

private:
    virtual void convertHToBE() = 0;
};

class ResponseHeaderV0 : public ResponseHeader
{
public:
    ResponseHeaderV0() = default;
    void respond(int client_fd) override;

private:
    void convertHToBE() override;

    int32_t response_msg_size;
    int32_t response_corr_id;

    friend ResponseMessage processAPIVersions(const RequestHeaderV2 &request_header, const APIVersionsRequestBodyV4 &request_body);
};

class ResponseHeaderV1 : public ResponseHeader
{
public:
    ResponseHeaderV1() = default;
    void respond(int client_fd) override;

private:
    void convertHToBE() override;

    int32_t response_msg_size;
    int32_t response_corr_id;
    int8_t tag_buffer;

    friend ResponseMessage processDescribeTopicPartitions(const RequestHeaderV2 &request_header, const DescribeTopicPartitionsRequestBodyV0 &request_body);
};

class ResponseBody
{
public:
    virtual ~ResponseBody() {}
    virtual void respond(int client_fd) = 0;

private:
    virtual void convertHToBE() = 0;
};

class APIVersionsResponseBodyV4 : public ResponseBody
{
public:
    APIVersionsResponseBodyV4() = default;
    void respond(int client_fd) override;

public:
    struct APIVersion
    {
        int16_t api_key;
        int16_t api_min_ver;
        int16_t api_max_ver;
        int8_t tag_buffer;

        static constexpr size_t size() { return sizeof(api_key) + sizeof(api_min_ver) + sizeof(api_max_ver) + sizeof(tag_buffer); }
    };

private:
    void convertHToBE() override;

    int16_t error_code;
    int8_t api_versions_array_len;
    std::vector<APIVersion> api_versions_array; // Kafka Compact arry (N+1)
    int32_t throttle_time;
    int8_t tag_buffer;

    friend ResponseMessage processAPIVersions(const RequestHeaderV2 &request_header, const APIVersionsRequestBodyV4 &request_body);
};

class DescribeTopicPartitionsResponseBodyV0 : public ResponseBody
{
public:
    DescribeTopicPartitionsResponseBodyV0() = default;
    void respond(int client_fd) override;

public:
    struct Topic
    {
        struct Partition
        {
            int16_t error_code;
            int32_t partition_index;
            int32_t leader_id;
            int32_t leader_epoch;
            int8_t replica_nodes_array_len;
            std::vector<int32_t> replica_nodes_array; // Kafka Compact arry (N+1)
            int8_t isr_nodes_array_len;
            std::vector<int32_t> isr_nodes_array; // Kafka Compact arry (N+1)
            int8_t elr_nodes_array_len;
            std::vector<int32_t> elr_nodes_array; // Kafka Compact arry (N+1)
            int8_t last_known_elr_nodes_array_len;
            std::vector<int32_t> last_known_elr_nodes_array; // Kafka Compact arry (N+1)
            int8_t offline_replica_nodes_array_len;
            std::vector<int32_t> offline_replica_nodes_array; // Kafka Compact arry (N+1)
            int8_t tag_buffer;

            size_t size() const { return sizeof(error_code) + sizeof(partition_index) + sizeof(leader_id) + sizeof(leader_epoch) +
                                         sizeof(replica_nodes_array_len) + replica_nodes_array.size() * sizeof(int32_t) +
                                         sizeof(isr_nodes_array_len) + isr_nodes_array.size() * sizeof(int32_t) +
                                         sizeof(elr_nodes_array_len) + elr_nodes_array.size() * sizeof(int32_t) +
                                         sizeof(last_known_elr_nodes_array_len) + last_known_elr_nodes_array.size() * sizeof(int32_t) +
                                         sizeof(offline_replica_nodes_array_len) + offline_replica_nodes_array.size() * sizeof(int32_t) +
                                         sizeof(tag_buffer); }
        };

        int16_t error_code;
        int8_t topic_name_len;
        std::vector<char> topic_name; // Kafka Compact string (N+1)
        UUID topic_id;
        int8_t is_internal;
        int8_t partitions_array_len;
        std::vector<Partition> partitions_array; // Kafka Compact arry (N+1)
        int32_t topic_authorized_ops;
        int8_t tag_buffer;

        size_t size() const
        {
            return sizeof(error_code) + sizeof(topic_name_len) + topic_name.size() +
                   sizeof(topic_id) + sizeof(is_internal) + sizeof(partitions_array_len) +
                   std::accumulate(partitions_array.begin(), partitions_array.end(), 0, [](size_t sum, const Partition &p)
                                   { return sum + p.size(); }) +
                   sizeof(topic_authorized_ops) + sizeof(tag_buffer);
        }
    };

private:
    void convertHToBE() override;

    int32_t throttle_time;
    int8_t topics_array_len;
    std::vector<Topic> topics_array; // Kafka Compact arry (N+1)
    int8_t next_cursor;
    int8_t tag_buffer;

    friend ResponseMessage processDescribeTopicPartitions(const RequestHeaderV2 &request_header, const DescribeTopicPartitionsRequestBodyV0 &request_body);
};

ResponseMessage processAPIVersions(const RequestHeaderV2 &request_header, const APIVersionsRequestBodyV4 &request_body);
ResponseMessage processDescribeTopicPartitions(const RequestHeaderV2 &request_header, const DescribeTopicPartitionsRequestBodyV0 &request_body);