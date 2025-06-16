from kafka.admin import KafkaAdminClient, NewTopic
from threading import Lock

from bank_connect_kafka_consumers.conf import KAFKA_BROKER_URLS
class KafkaAdminSingleton:
    _instance = None
    _lock = Lock()

    def __new__(cls, **kwargs):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super(KafkaAdminSingleton, cls).__new__(cls)
                    cls._instance._initialize( **kwargs)
        return cls._instance

    def _initialize(self,  **kwargs):
        self.admin_client = KafkaAdminClient(
            bootstrap_servers= KAFKA_BROKER_URLS,
            security_protocol="SSL",
            api_version=(0, 10, 1),
        )

    def create_topic(self, name, num_partitions=4, replication_factor=3, **kwargs):
        topic = NewTopic(
            name=name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
            **kwargs
        )
        try:
            self.admin_client.create_topics([topic])
            return True, name
        except Exception as e:
            print(f"Exceptio in creating topic: {e}")
            return False, ''

    def list_topics(self):
        return self.admin_client.list_topics()

    def close(self):
        self.admin_client.close()