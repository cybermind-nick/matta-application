from kafka import KafkaAdminClient
from kafka.admin import NewTopic

class SensorStreamAdmin:
    def __init__(self, brokers):
        self.admin = KafkaAdminClient(
            bootstrap_servers=brokers,
            sasl_plain_username="factory-sensors",
            sasl_plain_password="<password>",
        )

    def topic_exists(self, topic: str) -> bool:
        topics_info_list = self.admin.list_topics()
        return topic in topics_info_list
    
    def create_topic(self, topic_name, num_partitions=1, replication_factor=1):
        if not self.topic_exists(topic_name):
            new_topic = NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
            self.admin.create_topics([new_topic])
            # Log topic creation
        else:
            print(f"Topic: {topic_name} already exists")
    
    def close(self):
        self.admin.close()