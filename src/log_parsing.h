#pragma once

#include "common.h"
#include "kafka_utils.h"

class FeatureLevelRecord;
class TopicRecord;
class PartitionRecord;

class Record;
class RecordBatch;

class Varint
{
public:
    Varint() = default;
    void readValue(std::ifstream &file)
    {
        int8_t val;
        constexpr int8_t NO_MSB = 0x7F;
        std::stack<int8_t> varint_elems;
        do
        {
            file.read(reinterpret_cast<char *>(&val), sizeof(val));
            varint_elems.push(val);
        } while (val < 0); // For leading bit 0 check

        assert(varint_elems.size() > 0 && varint_elems.size() <= 4); // Only handling int32_t

        varint = varint_elems.top();
        varint_elems.pop();

        while (!varint_elems.empty())
        {
            varint = (varint << 7) | (varint_elems.top() & NO_MSB);
            varint_elems.pop();
        }

        varint = (varint >> 1) ^ -(varint & 1);
    }
    int32_t getValue() const
    {
        return varint;
    }

private:
    int32_t varint;
};

class UnsignedVarint
{
public:
    UnsignedVarint() = default;
    void readValue(std::ifstream &file)
    {
        uint8_t val;
        constexpr uint8_t NO_MSB = 0x7F;
        std::stack<uint8_t> unsigned_varint_elems;
        do
        {
            file.read(reinterpret_cast<char *>(&val), sizeof(val));
            unsigned_varint_elems.push(val);
        } while (val > 127); // For leading bit 0 check

        assert(unsigned_varint_elems.size() > 0 && unsigned_varint_elems.size() <= 4); // Only handling uint32_t

        unsigned_varint = unsigned_varint_elems.top();
        unsigned_varint_elems.pop();

        while (!unsigned_varint_elems.empty())
        {
            unsigned_varint = (unsigned_varint << 7) | (unsigned_varint_elems.top() & NO_MSB);
            unsigned_varint_elems.pop();
        }
    }
    uint32_t getValue() const
    {
        return unsigned_varint;
    }

private:
    uint32_t unsigned_varint;
};

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
    UnsignedVarint name_length;
    std::vector<char> name;
    int16_t feature_level;
    UnsignedVarint tagged_fields_count;

    friend DescribeTopicPartitionsResponseBodyV0::Topic LogParser::extractTopicPartitionRecords(int8_t topic_name_len, const std::vector<char> &topic_name);
};

class TopicRecord : public RecordValue
{
public:
    TopicRecord(std::ifstream &file, int8_t frame_version_, int8_t type_, int8_t version_);
    RECORD_VALUE getRecordType() override { return RECORD_VALUE::TOPIC; }

private:
    UnsignedVarint name_length;
    std::vector<char> topic_name;
    UUID topic_id;
    UnsignedVarint tagged_fields_count;

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
    UnsignedVarint replica_array_len;
    std::vector<int32_t> replica_array; // Kafka Compact arry (N+1)
    UnsignedVarint isr_array_len;
    std::vector<int32_t> isr_array; // Kafka Compact arry (N+1)
    UnsignedVarint rr_array_len;
    std::vector<int32_t> rr_array; // Kafka Compact arry (N+1)
    UnsignedVarint ar_array_len;
    std::vector<int32_t> ar_array; // Kafka Compact arry (N+1)
    int32_t leader;
    int32_t leader_epoch;
    int32_t partition_epoch;
    UnsignedVarint directories_array_len;
    std::vector<UUID> directories_array; // Kafka Compact arry (N+1)
    UnsignedVarint tagged_fields_count;

    friend DescribeTopicPartitionsResponseBodyV0::Topic LogParser::extractTopicPartitionRecords(int8_t topic_name_len, const std::vector<char> &topic_name);
};

class Record
{
public:
    Record(std::ifstream &file);

private:
    Varint length;
    int8_t attributes;
    Varint timestamp_delta;
    Varint offset_delta;
    Varint key_length;
    std::vector<int8_t> key;
    Varint value_length;
    std::unique_ptr<RecordValue> value;
    UnsignedVarint headers_array_count;

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
