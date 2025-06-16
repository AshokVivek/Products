import os
import time
import importlib
import traceback
from multiprocessing import Process
import json
from kafka import KafkaConsumer
from kafka.errors import CommitFailedError
from concurrent.futures import ThreadPoolExecutor

from conf import (KAFKA_BROKER_URLS)


class KafkaConsumerManager:
    def __init__(self, topic=None, group_id=None, consumer_target_function=None, num_workers=4, num_threads=4, is_batch_consumption_enabled=False, message_consumption_batch_size=10, max_poll_interval_in_ms=60000):

        # List of topics to consume, Add Topic name here and match the case below
        if topic is None:
            raise ValueError('Topic cannot be None')
        self.topic = topic
        if group_id is None:
            raise ValueError('group_id cannot be None')
        self.group_id = group_id
        if consumer_target_function is None:
            raise ValueError('consumer_target_function cannot be None')
        self.consumer_target_function = consumer_target_function
        self.consumer_function_module_name = f'bank_connect_kafka_consumers.consumers.{self.consumer_target_function}'
        self.num_workers = int(num_workers)
        self.num_threads = int(num_threads)
        self.message_consumption_batch_size = int(message_consumption_batch_size)
        self.is_batch_consumption_enabled = is_batch_consumption_enabled
        self.max_poll_interval_in_ms = int(max_poll_interval_in_ms)
        self.session_timeout_ms = int(max_poll_interval_in_ms) // 3 # this value should be 1/3rd of the max_poll_interval_time
        self.executor = ThreadPoolExecutor(max_workers=self.num_threads)

    def dynamic_function_import(self, module_name, function_name):
        module = importlib.import_module(module_name)
        func = getattr(module, function_name)
        return func

    def get_kafka_consumer(self, client_id=""):
        return KafkaConsumer(
            client_id=client_id,
            bootstrap_servers=KAFKA_BROKER_URLS,
            api_version=(0, 10, 1),
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            retry_backoff_ms=1000,
            reconnect_backoff_ms=10000,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            group_id=self.group_id,
            security_protocol="SSL",
            max_poll_interval_ms=self.max_poll_interval_in_ms,
            session_timeout_ms=self.session_timeout_ms,
            max_poll_records=self.num_threads if not self.is_batch_consumption_enabled else self.message_consumption_batch_size # max records returned per poll
        )

    def start_consumer(self):
        total_workers = []
        while True:
            num_alive_workers = len([worker for worker in total_workers if worker.is_alive()])

            if self.num_workers == num_alive_workers:
                time.sleep(10)
                continue

            for _ in range(self.num_workers - num_alive_workers):
                p = Process(target=self._consumer_function, daemon=True)
                p.start()
                total_workers.append(p)
                print('Starting worker {}'.format(p.pid))


    def _consumer_function(self):
        print(
            'Starting consumer group={}, topic={}, group_id={}'.format(os.getpid(), self.topic, self.group_id)
        )
        consumer_target_function = self.dynamic_function_import(self.consumer_function_module_name,
                                                                self.consumer_target_function)

        consumer = self.get_kafka_consumer(f"consumer-{os.getpid()}-{self.topic}")
        consumer.subscribe([self.topic])

        while True:
            message_found=False
            print('Worker ID:{} - Waiting for message...'.format(os.getpid()))

            try:
                msg_pack = consumer.poll()
                for topic_partition, messages in msg_pack.items():
                    message_found = True
                    if self.is_batch_consumption_enabled:
                        # if batch mode is enabled, accumulate polled messages and then start a thread
                        batched_messages = [msg.value for msg in messages]
                        future = self.executor.submit(self._process_message, consumer_target_function, batched_messages)
                        future.add_done_callback(lambda f: self._commit(consumer))
                    else:
                        #if not batch mode, open a thread for each message
                        for msg in messages:
                            # Submit the message processing to the thread pool
                            future = self.executor.submit(self._process_message, consumer_target_function, msg.value)
                            future.add_done_callback(lambda f: self._commit(consumer))

            except Exception:
                print('{} - Worker terminated with error: \n{}'.format(os.getpid(), traceback.print_exc()))
                consumer.commit()
                consumer.close()
                break

            # sleep only if no messages were found in this iteration
            if not message_found:
                time.sleep(10)

    def _process_message(self, consumer_target_function, message):
        try:
            output = consumer_target_function(message)
            print(f"Worker ID:{os.getpid()}, Message output:- ", output)
        except Exception as e:
            print(f'Worker ID:{os.getpid()}, Error processing message: {e}')
            #  TODO: Retry logic to be added here

    def _commit(self, consumer):
        try:
            print('Worker ID:{} - Attempting to commit message...'.format(os.getpid()))
            consumer.commit()
            print('Worker ID:{} - Committed message Successfully...'.format(os.getpid()))
        except CommitFailedError as e:
            print('Commit failed: {}'.format(e))





