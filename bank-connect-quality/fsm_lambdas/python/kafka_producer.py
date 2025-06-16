import json
from python.configs import KAFKA_BROKERS, LAMBDA_LOGGER, IS_SERVER

kafka_producer = None 
if IS_SERVER:
    from confluent_kafka import Producer, KafkaException
    KAFKA_CONFIG = {
        "bootstrap.servers": KAFKA_BROKERS,
        "compression.type": "lz4",
        "security.protocol": "SSL"
    }
    kafka_producer = Producer(KAFKA_CONFIG)

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        LAMBDA_LOGGER.info('Failed to send data to kafka as: {}'.format(err))

def send_data_to_kafka(topic_name, payload):
    #json dumps
    json_payload = json.dumps(payload)  
    
    #Trigger any available delivery report callbacks from previous produce() calls
    kafka_producer.poll(0)

    # Asynchronously produce a message. The delivery report callback will
    # be triggered from the call to poll() above, or flush() below, when the
    # message has been successfully delivered or failed permanently.
    kafka_producer.produce(topic_name, json_payload.encode('utf-8'), callback=delivery_report)

    # Wait for any outstanding messages to be delivered and delivery report
    # callbacks to be triggered.
    kafka_producer.flush()
    return True

def send_large_list_payload_to_kafka(payload: list, topic_name, depth = 0):
    # base condition
    if depth > 10:
        raise Exception("Failed to send data to kafka as Exceeded max number of division on kafka data streaming")

    #poll on initial entry for any available delivery report callbacks from previous produce() calls 
    if depth==0:
        kafka_producer.poll(0)

    try:
        json_payload = json.dumps(payload)
        kafka_producer.produce(topic_name, json_payload.encode('utf-8'), callback=delivery_report) 
    except KafkaException as e:
        if e.args and e.args[0].code() == 10: # KafkaError{code=MSG_SIZE_TOO_LARGE}
            # divide the paylaod into two payloads
            mid_index = int(len(payload) / 2)
            send_large_list_payload_to_kafka(payload[:mid_index], topic_name, depth+1)
            send_large_list_payload_to_kafka(payload[mid_index:], topic_name, depth+1)
        else:
            LAMBDA_LOGGER.info(f"Failed to send data to kafka as KafkaException {str(e)}")
    except Exception as e:
        LAMBDA_LOGGER.info(f"Failed to send data to kafka as {str(e)}")
        # raise e

    # Wait for any outstanding messages to be delivered and delivery report
    # callbacks to be triggered once all data is streamed after division
    if depth == 0:
        kafka_producer.flush()
    return