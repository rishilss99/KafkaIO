#pragma once

#include "common.h"
#include "kafka_utils.h"

class FeatureLevelRecord;
class TopicRecord;
class PartitionRecord;

class Record;
class RecordBatch;

class LogParser
{
public:
    LogParser(const std::string &file_path) : file(file_path, std::ios::binary)
    {
        if (!file.is_open())
        {
            assert(true); // Should be able to open file
        }
    }

    DescribeTopicPartitionsResponseBodyV0::Topic extractTopicPartitionRecords(int8_t topic_name_len, const std::vector<char> &topic_name);

private:
    std::ifstream file;
};

class RecordValue
{
public:
    enum class RECORD_VALUE
    {
        FEATURE_LEVEL,
        TOPIC,
        PARTITION
    };
    static std::unique_ptr<RecordValue> parseRecordValue(std::ifstream &file);
    virtual RECORD_VALUE getRecordType() = 0;
    virtual ~RecordValue() {}

protected:
    RecordValue(int8_t frame_version_, int8_t type_, int8_t version_) : frame_version(frame_version_), type(type_), version(version_) {}
    int8_t frame_version;
    int8_t type;
    int8_t version;
};

class FeatureLevelRecord : public RecordValue
{
public:
    FeatureLevelRecord(std::ifstream &file, int8_t frame_version_, int8_t type_, int8_t version_);
    RECORD_VALUE getRecordType() override { return RECORD_VALUE::FEATURE_LEVEL; }

private:
    int8_t name_length;
    std::vector<char> name;
    int16_t feature_level;
    int8_t tagged_fields_count;

    friend DescribeTopicPartitionsResponseBodyV0::Topic LogParser::extractTopicPartitionRecords(int8_t topic_name_len, const std::vector<char> &topic_name);
};

class TopicRecord : public RecordValue
{
public:
    TopicRecord(std::ifstream &file, int8_t frame_version_, int8_t type_, int8_t version_);
    RECORD_VALUE getRecordType() override { return RECORD_VALUE::TOPIC; }

private:
    int8_t name_length;
    std::vector<char> topic_name;
    UUID topic_id;
    int8_t tagged_fields_count;

    friend DescribeTopicPartitionsResponseBodyV0::Topic LogParser::extractTopicPartitionRecords(int8_t topic_name_len, const std::vector<char> &topic_name);
};

class PartitionRecord : public RecordValue
{
public:
    PartitionRecord(std::ifstream &file, int8_t frame_version_, int8_t type_, int8_t version_);
    RECORD_VALUE getRecordType() override { return RECORD_VALUE::PARTITION; }

private:
    int32_t partition_id;
    UUID topic_id;
    int8_t replica_array_len;
    std::vector<int32_t> replica_array; // Kafka Compact arry (N+1)
    int8_t isr_array_len;
    std::vector<int32_t> isr_array; // Kafka Compact arry (N+1)
    int8_t rr_array_len;
    std::vector<int32_t> rr_array; // Kafka Compact arry (N+1)
    int8_t ar_array_len;
    std::vector<int32_t> ar_array; // Kafka Compact arry (N+1)
    int32_t leader;
    int32_t leader_epoch;
    int32_t partition_epoch;
    int8_t directories_array_len;
    std::vector<UUID> directories_array; // Kafka Compact arry (N+1)
    int8_t tagged_fields_count;

    friend DescribeTopicPartitionsResponseBodyV0::Topic LogParser::extractTopicPartitionRecords(int8_t topic_name_len, const std::vector<char> &topic_name);
};

class Record
{
public:
    Record(std::ifstream &file);

private:
    class Varint
    {
    public:
        Varint() : varint_pair(0, 0) {}
        void readValue(std::ifstream &file)
        {
            file.read(reinterpret_cast<char *>(&varint_pair.first), sizeof(varint_pair.first));
            if (varint_pair.first >= 0) // For leading but 0 check
            {
                file.read(reinterpret_cast<char *>(&varint_pair.second), sizeof(varint_pair.second));
            }
        }
        int16_t getValue()
        {
            int16_t value = varint_pair.second;
            value = (value << 8) + varint_pair.first;
            return value;
        }

    private:
        std::pair<int8_t, int8_t> varint_pair;
    };

private:
    int8_t length;
    int8_t attributes;
    int8_t timestamp_delta;
    int8_t offset_delta;
    int8_t key_length;
    std::vector<int8_t> key;
    Varint value_length;
    std::unique_ptr<RecordValue> value;
    int8_t headers_array_count;

    friend DescribeTopicPartitionsResponseBodyV0::Topic LogParser::extractTopicPartitionRecords(int8_t topic_name_len, const std::vector<char> &topic_name);
};

class RecordBatch
{
public:
    RecordBatch(std::ifstream &file);

private:
    int64_t base_offset;
    int32_t batch_length;
    int32_t partition_leader_epoch;
    int8_t magic_byte;
    int32_t crc;
    int16_t attributes;
    int32_t last_offset_delta;
    int64_t base_timestamp;
    int64_t max_timestamp;
    int64_t producer_id;
    int16_t producer_epoch;
    int32_t base_sequence;
    int32_t records_length;
    std::vector<std::unique_ptr<Record>> records;

    friend DescribeTopicPartitionsResponseBodyV0::Topic LogParser::extractTopicPartitionRecords(int8_t topic_name_len, const std::vector<char> &topic_name);
};
