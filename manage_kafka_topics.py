class KafkaTopicManager():
    def __init__(self, HOST, PORT):
        from kafka.admin import KafkaAdminClient, NewTopic
        self.kafka_admin = KafkaAdminClient(bootstrap_servers = f"{HOST}:{PORT}")

    def create_topic(self, topic_name: str, num_partitions: int = 1, replication_factor:int = 1):
        try:
            topic = NewTopic(topic_name, num_partitions = 1, replication_factor = 1)
            self.kafka_admin.create_topics([topic], validate_only = True)
            print(self.kafka_admin.create_topics([topic]))
        except:
            print("Kafka Topic previously created is available")
            pass
    
    def delete_topic(self, topic_name):
        try:
            self.kafka_admin.delete_topics([topic_name])
            print(f"Kafka Topic {topic_name} Deleted")
        except:
            print("Kafka Topic does not exist")