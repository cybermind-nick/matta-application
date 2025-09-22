from kafka import KafkaProducer
import json

class SensorStreamProducer:
    def __init__(self, brokers, topic):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=brokers,
            sasl_plain_username="factory-sensors",
            sasl_plain_password="<password>",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )

    def write_message(self, factory_id, sensor_id,  data):
        self.producer.send(self.topic, {"topic": self.topic, "factory_id": factory_id,"sensor_id": sensor_id, "data": data})
        self.producer.flush()
    def close(self):
        self.producer.close()