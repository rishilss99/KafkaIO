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
    void printDump() const
    {
        std::stringstream ss;

        ss << "FeatureLevelRecord" << "\n"
           << "Frame version: " << static_cast<int>(frame_version) << "\n"
           << "Type: " << static_cast<int>(type) << "\n"
           << "Version: " << static_cast<int>(version) << "\n"
           << "Name Length: " << name_length.getValue() << "\n"
           << "Name: " << std::string(name.begin(), name.end()) << "\n"
           << "Feature Level: " << static_cast<int>(feature_level) << "\n"
           << "Tagged Fields Count: " << tagged_fields_count.getValue() << "\n";

        std::cout << ss.str();
    }

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
    void printDump() const
    {
        std::stringstream ss;

        ss << "TopicRecord" << "\n"
           << "Frame version: " << static_cast<int>(frame_version) << "\n"
           << "Type: " << static_cast<int>(type) << "\n"
           << "Version: " << static_cast<int>(version) << "\n"
           << "Name Length: " << name_length.getValue() << "\n"
           << "Topic Name: " << std::string(topic_name.begin(), topic_name.end()) << "\n"
           << "Topic ID: ";

        std::for_each(topic_id.begin(), topic_id.end(), [&ss](const uint8_t &byte)
                      { ss << static_cast<uint32_t>(byte); });

        ss << "\n"
           << "Tagged Fields Count: " << tagged_fields_count.getValue() << "\n";

        std::cout << ss.str();
    }

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
    void printDump() const
    {
        std::stringstream ss;
        ss << "PartitionRecord" << "\n"
           << "Frame version: " << static_cast<int>(frame_version) << "\n"
           << "Type: " << static_cast<int>(type) << "\n"
           << "Version: " << static_cast<int>(version) << "\n"
           << "Partition ID: " << partition_id << "\n"
           << "Topic ID: ";
        std::for_each(topic_id.begin(), topic_id.end(), [&ss](const uint8_t &byte)
                      { ss << static_cast<uint32_t>(byte); });
        ss << "\n"
           << "Replica Array Length: " << replica_array_len.getValue() << "\n"
           << "Replica Array: ";
        std::for_each(replica_array.begin(), replica_array.end(), [&ss](const int32_t &replica)
                      { ss << replica << " "; });
        ss << "\n"
           << "ISR Array Length: " << isr_array_len.getValue() << "\n"
           << "ISR Array: ";
        std::for_each(isr_array.begin(), isr_array.end(), [&ss](const int32_t &isr)
                      { ss << isr << " "; });
        ss << "\n"
           << "RR Array Length: " << rr_array_len.getValue() << "\n"
           << "RR Array: ";
        std::for_each(rr_array.begin(), rr_array.end(), [&ss](const int32_t &rr)
                      { ss << rr << " "; });
        ss << "\n"
           << "AR Array Length: " << ar_array_len.getValue() << "\n"
           << "AR Array: ";
        std::for_each(ar_array.begin(), ar_array.end(), [&ss](const int32_t &ar)
                      { ss << ar << " "; });
        ss << "\n"
           << "Leader: " << leader << "\n"
           << "Leader Epoch: " << leader_epoch << "\n"
           << "Partition Epoch: " << partition_epoch << "\n"
           << "Directories Array Length: " << directories_array_len.getValue() << "\n"
           << "Directories Array: ";
        std::for_each(directories_array.begin(), directories_array.end(), [&ss](const UUID &uuid)
                      {
        ss << "[";
        
        std::for_each(uuid.begin(), uuid.end(), [&ss](const uint8_t &byte)
        { ss << static_cast<uint32_t>(byte); });
        
        ss << "] "; });

        ss << "\n"
           << "Tagged Fields Count: " << tagged_fields_count.getValue() << "\n";

        std::cout << ss.str();
    }

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
    void printDump() const
    {
        std::stringstream ss;
        ss << "Record" << "\n"
           << "Length: " << length.getValue() << "\n"
           << "Attributes: " << std::bitset<8>(attributes) << "\n"
           << "Timestamp Delta: " << timestamp_delta.getValue() << "\n"
           << "Offset Delta: " << offset_delta.getValue() << "\n"
           << "Key Length: " << key_length.getValue() << "\n"
           << "Key: ";

        // For each loop to print key bytes
        std::for_each(key.begin(), key.end(), [&ss](const int8_t &byte)
                      { ss << static_cast<int>(byte) << " "; });

        ss << "\n"
           << "Value Length: " << value_length.getValue() << "\n"
           << "Headers Array Count: " << headers_array_count.getValue() << "\n";

        std::cout << ss.str();
    }

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
    void printDump() const
    {
        std::stringstream ss;
        ss << "RecordBatch" << "\n"
           << "Base Offset: " << base_offset << "\n"
           << "Batch Length: " << batch_length << "\n"
           << "Partition Leader Epoch: " << partition_leader_epoch << "\n"
           << "Magic Byte: " << std::bitset<8>(magic_byte) << "\n" // Cast to int to print numeric value
           << "CRC: " << crc << "\n"
           << "Attributes: " << attributes << "\n"
           << "Last Offset Delta: " << last_offset_delta << "\n"
           << "Base Timestamp: " << base_timestamp << "\n"
           << "Max Timestamp: " << max_timestamp << "\n"
           << "Producer ID: " << producer_id << "\n"
           << "Producer Epoch: " << producer_epoch << "\n"
           << "Base Sequence: " << base_sequence << "\n"
           << "Records Length: " << records_length << "\n";

        std::cout << ss.str();
    }

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
