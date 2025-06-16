import sys
import os
import traceback
from sentry_sdk import capture_exception

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)

from kafka_consumer_manager import KafkaConsumerManager

try:
    TOPIC_NAME = os.environ.get("TOPIC_NAME", "")
    GROUP_ID = os.environ.get("GROUP_ID", "")
    CONSUMER_FUNCTION_NAME = os.environ.get("CONSUMER_FUNCTION_NAME", "")
    NUMBER_OF_WORKERS = os.environ.get("NUMBER_OF_WORKERS", 4)
    NUMBER_OF_THREADS = os.environ.get("NUMBER_OF_THREADS", 4)
    IS_BATCH_CONSUMPTION_ENABLED = os.environ.get("IS_BATCH_CONSUMPTION_ENABLED", False)
    MESSAGE_CONSUMPTION_BATCH_SIZE = os.environ.get("MESSAGE_CONSUMPTION_BATCH_SIZE","10")
    MAX_POLL_INTERVAL_IN_MILLISECONDS = os.environ.get("MAX_POLL_INTERVAL_IN_MILLISECONDS", "60000")


    if not TOPIC_NAME or not GROUP_ID or not CONSUMER_FUNCTION_NAME:
        raise Exception("TOPIC_NAME, GROUP_ID and CONSUMER_FUNCTION_NAME are Required as environment variables")

    assert isinstance(TOPIC_NAME,str)
    assert isinstance(GROUP_ID,str)
    assert isinstance(CONSUMER_FUNCTION_NAME,str)
    assert NUMBER_OF_THREADS.isdigit()
    assert NUMBER_OF_WORKERS.isdigit()
    assert IS_BATCH_CONSUMPTION_ENABLED in ['true', 'false', False, True]
    assert MESSAGE_CONSUMPTION_BATCH_SIZE.isdigit()
    assert MAX_POLL_INTERVAL_IN_MILLISECONDS.isdigit()

    MAX_POLL_INTERVAL_IN_MILLISECONDS = max(int(MAX_POLL_INTERVAL_IN_MILLISECONDS), 60000) # This value should be minimum of 60000


    if IS_BATCH_CONSUMPTION_ENABLED.lower() == 'false':
        IS_BATCH_CONSUMPTION_ENABLED = False
    elif IS_BATCH_CONSUMPTION_ENABLED.lower() == 'true':
        IS_BATCH_CONSUMPTION_ENABLED = True

    # start consumer
    kafka_consumer_manager = KafkaConsumerManager(topic=TOPIC_NAME,
                                                  group_id=GROUP_ID,
                                                  consumer_target_function=CONSUMER_FUNCTION_NAME,
                                                  num_workers=NUMBER_OF_WORKERS,
                                                  num_threads=NUMBER_OF_THREADS,
                                                  is_batch_consumption_enabled=IS_BATCH_CONSUMPTION_ENABLED,
                                                  message_consumption_batch_size=MESSAGE_CONSUMPTION_BATCH_SIZE,
                                                  max_poll_interval_in_ms=MAX_POLL_INTERVAL_IN_MILLISECONDS)

    kafka_consumer_manager.start_consumer()
except Exception as e:
    print(traceback.format_exc())
    capture_exception(e)

