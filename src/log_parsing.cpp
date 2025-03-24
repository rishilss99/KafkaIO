#include "log_parsing.h"

static void readCompactString(std::ifstream &file, UnsignedVarint &len, std::vector<char> &str)
{
    len.readValue(file);
    str.resize(len.getValue() - 1);
    file.read(str.data(), len.getValue() - 1);
}

FeatureLevelRecord::FeatureLevelRecord(std::ifstream &file, int8_t frame_version_, int8_t type_, int8_t version_) : RecordValue(frame_version_, type_, version_)
{
    readCompactString(file, name_length, name);
    file.read(reinterpret_cast<char *>(&feature_level), sizeof(feature_level));
    tagged_fields_count.readValue(file);

    convertBE16toH(feature_level);
}

TopicRecord::TopicRecord(std::ifstream &file, int8_t frame_version_, int8_t type_, int8_t version_) : RecordValue(frame_version_, type_, version_)
{
    readCompactString(file, name_length, topic_name);
    file.read(reinterpret_cast<char *>(topic_id.data()), topic_id.size());
    tagged_fields_count.readValue(file);
}

PartitionRecord::PartitionRecord(std::ifstream &file, int8_t frame_version_, int8_t type_, int8_t version_) : RecordValue(frame_version_, type_, version_)
{
    file.read(reinterpret_cast<char *>(&partition_id), sizeof(partition_id));
    file.read(reinterpret_cast<char *>(topic_id.data()), topic_id.size());

    replica_array_len.readValue(file);
    int32_t replica_id;
    for (int i = 0; i < replica_array_len.getValue() - 1; i++)
    {
        file.read(reinterpret_cast<char *>(&replica_id), sizeof(replica_id));
        convertBE32toH(replica_id);
        replica_array.push_back(replica_id);
    }

    isr_array_len.readValue(file);
    int32_t isr_id;
    for (int i = 0; i < isr_array_len.getValue() - 1; i++)
    {
        file.read(reinterpret_cast<char *>(&isr_id), sizeof(isr_id));
        convertBE32toH(isr_id);
        isr_array.push_back(isr_id);
    }

    rr_array_len.readValue(file);
    int32_t rr_id;
    for (int i = 0; i < rr_array_len.getValue() - 1; i++)
    {
        file.read(reinterpret_cast<char *>(&rr_id), sizeof(rr_id));
        convertBE32toH(rr_id);
        rr_array.push_back(rr_id);
    }

    ar_array_len.readValue(file);
    int32_t ar_id;
    for (int i = 0; i < ar_array_len.getValue() - 1; i++)
    {
        file.read(reinterpret_cast<char *>(&ar_id), sizeof(ar_id));
        convertBE32toH(ar_id);
        ar_array.push_back(ar_id);
    }

    file.read(reinterpret_cast<char *>(&leader), sizeof(leader));
    file.read(reinterpret_cast<char *>(&leader_epoch), sizeof(leader_epoch));
    file.read(reinterpret_cast<char *>(&partition_epoch), sizeof(partition_epoch));

    directories_array_len.readValue(file);
    UUID directory_uuid;
    for (int i = 0; i < directories_array_len.getValue() - 1; i++)
    {
        file.read(reinterpret_cast<char *>(directory_uuid.data()), directory_uuid.size());
        directories_array.push_back(directory_uuid);
    }

    tagged_fields_count.readValue(file);

    convertBE32toH(partition_id, leader, leader_epoch, partition_epoch);
}

std::unique_ptr<RecordValue> RecordValue::parseRecordValue(std::ifstream &file)
{
    int8_t frame_version_, type_, version_;

    file.read(reinterpret_cast<char *>(&frame_version_), sizeof(frame_version_));
    file.read(reinterpret_cast<char *>(&type_), sizeof(type_));
    file.read(reinterpret_cast<char *>(&version_), sizeof(version_));

    std::unique_ptr<RecordValue> record_value = nullptr;

    switch (type_)
    {
    case 12: // FeatureLevelRecord
        record_value = std::make_unique<FeatureLevelRecord>(file, frame_version_, type_, version_);
        break;

    case 2: // TopicRecord
        record_value = std::make_unique<TopicRecord>(file, frame_version_, type_, version_);
        break;

    case 3: // PartitionRecord
        record_value = std::make_unique<PartitionRecord>(file, frame_version_, type_, version_);
        break;

    default:
        assert(true); // No handling of unknown records
        break;
    }

    return record_value;
}

Record::Record(std::ifstream &file)
{
    length.readValue(file);
    file.read(reinterpret_cast<char *>(&attributes), sizeof(attributes));
    timestamp_delta.readValue(file);
    offset_delta.readValue(file);
    key_length.readValue(file);

    int8_t key_elem;
    for (int i = 0; i < key_length.getValue(); i++)
    {
        file.read(reinterpret_cast<char *>(&key_elem), sizeof(key_elem));
        key.push_back(key_elem);
    }

    value_length.readValue(file);

    assert(value_length.getValue() >= 3); // Atleast have the first 3 Bytes (frame_version, type, version)

    value = RecordValue::parseRecordValue(file);

    headers_array_count.readValue(file);

    printDump();

    value->printDump(); // Will always be valid since we will never have value as nullptr
}

RecordBatch::RecordBatch(std::ifstream &file)
{
    file.read(reinterpret_cast<char *>(&base_offset), sizeof(base_offset));
    file.read(reinterpret_cast<char *>(&batch_length), sizeof(batch_length));
    file.read(reinterpret_cast<char *>(&partition_leader_epoch), sizeof(partition_leader_epoch));
    file.read(reinterpret_cast<char *>(&magic_byte), sizeof(magic_byte));
    file.read(reinterpret_cast<char *>(&crc), sizeof(crc));
    file.read(reinterpret_cast<char *>(&attributes), sizeof(attributes));
    file.read(reinterpret_cast<char *>(&last_offset_delta), sizeof(last_offset_delta));
    file.read(reinterpret_cast<char *>(&base_timestamp), sizeof(base_timestamp));
    file.read(reinterpret_cast<char *>(&max_timestamp), sizeof(max_timestamp));
    file.read(reinterpret_cast<char *>(&producer_id), sizeof(producer_id));
    file.read(reinterpret_cast<char *>(&producer_epoch), sizeof(producer_epoch));
    file.read(reinterpret_cast<char *>(&base_sequence), sizeof(base_sequence));
    file.read(reinterpret_cast<char *>(&records_length), sizeof(records_length));

    convertBE16toH(attributes, producer_epoch);
    convertBE32toH(batch_length, partition_leader_epoch, crc, last_offset_delta, base_sequence, records_length);
    convertBE64toH(base_offset, base_timestamp, max_timestamp, producer_id);

    printDump();

    for (int i = 0; i < records_length; i++)
    {
        records.push_back(std::make_unique<Record>(file));
    }
}

DescribeTopicPartitionsResponseBodyV0::Topic LogParser::extractTopicPartitionRecords(int8_t topic_name_len, const std::vector<char> &topic_name)
{

    // Default Topic Not Found error response
    DescribeTopicPartitionsResponseBodyV0::Topic response_topic = {.error_code = 3, // Introduce macros for error codes
                                                                   .topic_name_len = topic_name_len,
                                                                   .topic_name = topic_name,
                                                                   .topic_id = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                                                                   .is_internal = 0,
                                                                   .partitions_array_len = 1,
                                                                   .topic_authorized_ops = 0,
                                                                   .tag_buffer = 0};
    bool topic_in_records = false;

    while (!file.eof())
    {
        RecordBatch temp_batch(file);

        for (auto &record : temp_batch.records)
        {
            if (record->value == nullptr)
                continue;

            if (record->value->getRecordType() == RecordValue::RECORD_VALUE::TOPIC)
            {
                const TopicRecord &topic_record = dynamic_cast<const TopicRecord &>(*(record->value));

                if (topic_name == topic_record.topic_name)
                {
                    response_topic.error_code = 0;
                    response_topic.topic_id = topic_record.topic_id;
                    topic_in_records = true;
                }
            }
            else if (record->value->getRecordType() == RecordValue::RECORD_VALUE::PARTITION)
            {
                const PartitionRecord &partition_record = dynamic_cast<const PartitionRecord &>(*(record->value));

                if (topic_in_records && (partition_record.topic_id == response_topic.topic_id))
                {
                    DescribeTopicPartitionsResponseBodyV0::Topic::Partition response_partition = {.error_code = 0, // Introduce macros for error codes
                                                                                                  .partition_index = partition_record.partition_id,
                                                                                                  .leader_id = partition_record.leader,
                                                                                                  .leader_epoch = partition_record.leader_epoch,
                                                                                                  .replica_nodes_array_len = static_cast<int8_t>(partition_record.replica_array_len.getValue()),
                                                                                                  .replica_nodes_array = partition_record.replica_array,
                                                                                                  .isr_nodes_array_len = static_cast<int8_t>(partition_record.isr_array_len.getValue()),
                                                                                                  .isr_nodes_array = partition_record.isr_array,
                                                                                                  .elr_nodes_array_len = 1,
                                                                                                  .last_known_elr_nodes_array_len = 1,
                                                                                                  .offline_replica_nodes_array_len = 1,
                                                                                                  .tag_buffer = 0};

                    response_topic.partitions_array_len += 1;
                    response_topic.partitions_array.push_back(response_partition);
                }
            }
        }

        if(topic_in_records)
        {
            break; // Stop processing to avoid hitting the invalid RecordBatch
        }
    }

    file.seekg(0); // clear is implicit

    return response_topic;
}