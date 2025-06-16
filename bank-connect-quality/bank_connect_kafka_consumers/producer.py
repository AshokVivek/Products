from kafka import KafkaProducer
import json
from threading import Lock

from bank_connect_kafka_consumers.conf import KAFKA_BROKER_URLS

class KafkaProducerSingleton:
    _instance = None
    _lock = Lock()

    def __new__(cls, **kwargs):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    instance = super(KafkaProducerSingleton, cls).__new__(cls)
                    instance._initialize(**kwargs)
                    cls._instance = instance
        return cls._instance

    def _initialize(self, **kwargs):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER_URLS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            security_protocol="SSL"
        )

    def send(self, topic, value, key=None):
        success = True
        try:
            future = self.producer.send(topic, value=value, key=key)
            future.get(timeout=60)
            return success
        except Exception as e:
            success = False
            print("Send to failed in kafka", e)
            return success


    def flush(self):
        self.producer.flush()

    def close(self):
        self.producer.close()
