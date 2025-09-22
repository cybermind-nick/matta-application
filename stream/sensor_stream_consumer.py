from kafka import KafkaConsumer
import json

class SensorStreamConsumer:
    def __init__(self, brokers, topic, group_id):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=brokers,
            sasl_plain_username="factory-sensors",
            sasl_plain_password="<password>",
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )

    def consume_data(self):
        for msg in self.consumer:
            # write the message/data to a batch handler
            pass
    def close(self):
        self.consumer.close()