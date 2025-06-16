import json
from python.configs import *


def collect_results(table_f, qp):
    items = []
    while True:
        r = table_f(**qp)
        items.extend(r['Items'])
        lek = r.get('LastEvaluatedKey')
        if lek is None or lek == '':
            break
        qp['ExclusiveStartKey'] = lek
    return items


def get_json_from_s3_file(bucket_name, file_key):
    try:
        file_object = s3.get_object(
            Bucket=bucket_name,
            Key=file_key
        )
        
        json_object = json.loads(file_object["Body"].read().decode())
        return json_object
    except Exception as e:
        print("exception occured while getting json from s3 file, exception: {}".format(e))
        return None