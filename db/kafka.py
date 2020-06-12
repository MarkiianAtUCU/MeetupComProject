import json

from kafka import TopicPartition, KafkaConsumer


class OneTopicKafkaConsumer:
    def __init__(self, topic, bootstrap_servers):
        self.partition = TopicPartition(topic=topic, partition=0)
        self.kafka_consumer = KafkaConsumer(topic, bootstrap_servers=["localhost:9092"],
                                            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                                            auto_offset_reset="latest",
                                            enable_auto_commit=False)

    def get_last(self):
        end_offset = self.kafka_consumer.end_offsets([self.partition])[self.partition]
        self.kafka_consumer.seek(self.partition, end_offset - 1)
        return next(self.kafka_consumer).value
