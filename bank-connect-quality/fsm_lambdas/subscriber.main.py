import json
import os
import threading
import signal
import uuid

from sentry_sdk import capture_exception

from python.cc_utils_ocr import extract_cc_transactions, extract_cc_transactions_page
from python.configs import sqs_client, SUBSCRIPTION_TYPE, SUBSCRIBE_QUEUE_URL
from python.context.logging import LoggingContext
from python.handlers import extraction_helper, analyze_pdf_handler, analyze_transactions_finvu_aa, analyze_transactions_finvu_aa_page
from python.update_state_handlers import usfo_in_rams
from context.log_config import LOGGER

# Global shutdown flag
shutdown_flag = threading.Event()


def handle_sigterm(signum, frame):
    """
        SIGTERM - Polite way to ask a process to terminate, giving it a chance to clean up.
        A SIGTERM signal is registerd below that sets the shutdown flag, which means that new messages will not be consumed.
    """
    LOGGER.info("SIGTERM received. Initiating shutdown...")
    shutdown_flag.set()


def check_if_stop_polling_file_exists():
    """Check for stop signal file."""
    if os.path.exists("/tmp/stop_polling"):
        LOGGER.info("CONTAINER TERMINATION COMMAND ISSUED, STOP POLLING TO CONSUME NEW TASKS")
        return True
    return False


def operation(event, SUBSCRIPTION_TYPE, tracking_id):
    local_logging_context: LoggingContext = LoggingContext(source=f"operations module")
    local_logging_context.upsert(tracking_id=tracking_id)

    LOGGER.info(msg = f"Subscription Type : {SUBSCRIPTION_TYPE}", extra=local_logging_context.store)
    
    if SUBSCRIPTION_TYPE == "EXTRACTION":
        extraction_helper(event, local_logging_context=local_logging_context)

    elif SUBSCRIPTION_TYPE == "ANALYZE_PDF":
        event["local_logging_context"] = local_logging_context
        analyze_pdf_handler(event=event, context=None)
    
    elif SUBSCRIPTION_TYPE == "CREDIT_CARD_EXTRACTION":
        LOGGER.info(msg=f"Event received for {SUBSCRIPTION_TYPE}: {event}", extra=local_logging_context.store)
        event["local_logging_context"] = local_logging_context
        extract_cc_transactions(event=event, context=None)

    elif SUBSCRIPTION_TYPE == "CREDIT_CARD_EXTRACTION_PAGE":
        LOGGER.info(msg=f"Event received for {SUBSCRIPTION_TYPE}: {event}", extra=local_logging_context.store)
        event["local_logging_context"] = local_logging_context
        extract_cc_transactions_page(event = event, context = None)

    elif SUBSCRIPTION_TYPE == "AA_TRANSACTIONS_ORCHESTRATOR":
        print("AA Transactions Orchestration flow, processing messages")
        event["local_logging_context"] = local_logging_context
        analyze_transactions_finvu_aa(event=event, context=None)
    
    elif SUBSCRIPTION_TYPE == "AA_TRANSACTIONS_ORCHESTRATOR_PAGE":
        event["local_logging_context"] = local_logging_context
        analyze_transactions_finvu_aa_page(event=event, context=None)
    
    elif SUBSCRIPTION_TYPE == "RAMS_POST_PROCESSING":
        event["local_logging_context"] = local_logging_context
        usfo_in_rams(event, local_logging_context)
    
    else:
        LOGGER.error(msg = f"Invalid Subscription Type: {SUBSCRIPTION_TYPE}", extra=local_logging_context.store)
        raise Exception(f"Invalid Subscription Type: {SUBSCRIPTION_TYPE}")

def poll_and_process(queue_url, SUBSCRIPTION_TYPE):
    """
        Polls the SQS queue for messages and processes them.
    """
    local_logging_context: LoggingContext = LoggingContext(source=f"Poll and Process Subscriber, Queue: {queue_url}")

    LOGGER.info(
        msg=f"Trying to start a extraction process for consuming events for queue: {queue_url}",
        extra=local_logging_context.store
    )

    while not shutdown_flag.is_set() and not check_if_stop_polling_file_exists():
        # LOGGER.info(msg = "Trying to long poll, and wait time is 20 seconds", extra=local_logging_context.store)
        response = sqs_client.receive_message(
            QueueUrl = queue_url,
            MaxNumberOfMessages = 1,
            WaitTimeSeconds = 20
        )
        messages = response.get("Messages", [])
        if messages:
            for message in messages:
                tracking_id = str(uuid.uuid4())
                # LOGGER.info(msg = f"Message Received - {message}", extra=new_local_logging_context.store)
                local_logging_context.upsert(tracking_id=tracking_id)
                LOGGER.info("Message Received, Trying to process", extra = local_logging_context.store)
                _message_id = message["MessageId"]
                receipt_handle = message["ReceiptHandle"]
                event = json.loads(message["Body"])
                # LOGGER.info(msg=f"Event Received : {event}", extra=new_local_logging_context.store)
                
                success = False
                try:
                    operation(event, SUBSCRIPTION_TYPE, tracking_id)
                    success = True
                except Exception as e:
                    # this is done purposefully so that extraction does not break the next messages
                    LOGGER.error(msg=f"Exception Occurred: {e} for Received Event", extra=local_logging_context.store)
                    capture_exception(e)
                
                if success:
                    LOGGER.info(msg=f"Deleting Message with receipt handle {receipt_handle}", extra=local_logging_context.store)
                    try:
                        sqs_client.delete_message(
                            QueueUrl = queue_url,
                            ReceiptHandle = receipt_handle
                        )
                    except Exception as e:
                        LOGGER.error(msg=f"Error while deleting message :{event.get('key')} with receipt handle {receipt_handle} : {e}", extra=local_logging_context.store)

signal.signal(signal.SIGTERM, handle_sigterm)



if __name__ == "__main__":
    poll_and_process(queue_url=SUBSCRIBE_QUEUE_URL, SUBSCRIPTION_TYPE=SUBSCRIPTION_TYPE)