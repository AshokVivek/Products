import sys
import json
from time import sleep
from botocore.exceptions import ClientError, ConnectionClosedError

from python.configs import SIZE_EXCEEDED_ERROR_MESSAGE, streamClient


def send_data_to_stream_with_back_off(payload, streamName):
    attempt_count = 0
    success = False
    while attempt_count < 3 and not success:
        try:
            _ = streamClient.put_record(DeliveryStreamName=streamName, Record={"Data": payload})

            # print("Successfully put data in {} Firehose".format(streamName))

            success = True
            return

        except ClientError as e:
            print("Exception: {}".format(e))

            code = e.response["Error"]["Code"]
            _ = e.response["Error"]["Code"]

            if code in ["ServiceUnavailableException", "ProvisionedThroughputExceededException", "ThrottlingException"]:
                print("Handling {} error".format(str(e)))
            else:
                raise e

        except ConnectionClosedError as e:
            print("Handling {} error".format(str(e)))

        attempt_count += 1
        sleep(0.01 * (attempt_count + 1))

    raise Exception("Loop completed: attempt_count: {} & was success: {}".format(attempt_count, success))


def send_data_to_firehose(firehosePayload, STREAM_NAME):
    try:
        payload = json.dumps(firehosePayload)[1:-1]

        if sys.getsizeof(payload) > 1024000:
            mid_point_index = int(len(firehosePayload) / 2)
            send_data_to_firehose(firehosePayload[:mid_point_index], STREAM_NAME)
            send_data_to_firehose(firehosePayload[mid_point_index:], STREAM_NAME)
        else:
            send_data_to_stream_with_back_off(payload, STREAM_NAME)

    except ClientError as e:
        print(f"Error occurred while sending data = {e}")

        code = e.response["Error"]["Code"]
        message = e.response["Error"]["Message"]

        if (code == 'ValidationException' and SIZE_EXCEEDED_ERROR_MESSAGE in message) or code in (413, '413'):
            mid_point_index = int(len(firehosePayload) / 2)

            send_data_to_firehose(firehosePayload[:mid_point_index], STREAM_NAME)

            send_data_to_firehose(firehosePayload[mid_point_index:], STREAM_NAME)

            return

        print("Raising Exception")
        print("Code: {}".format(code))
        print("Message: {}".format(message))
        raise e
