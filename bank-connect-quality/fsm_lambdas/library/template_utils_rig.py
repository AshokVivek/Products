import os
import json
from uuid import uuid4
from json import JSONDecodeError

def assign_template_uuids():
    bank_json_files = os.listdir("./bank_data")

    available_keys = set()

    for json_file in bank_json_files:
        with open("./bank_data/"+json_file) as f:
            temp_dict = dict()
            try:
                temp_dict = json.loads(f.read())
            except Exception as e:
                # print("------- could not read the file: {} -------".format(json_file))
                pass

            for key in temp_dict.keys():
                available_keys.add(key)

    print(available_keys)
    # do not process
    # removing "account_category_mapping" from available keys
    available_keys.remove("account_category_mapping")

    # for every file
    for json_file in bank_json_files:
        with open("./bank_data/" + json_file) as f:
            print("processing: {}".format(json_file))
            temp_dict = dict()
            try:
                temp_dict = json.loads(f.read())
            except Exception as e:
                # print("------- could not modify the file: {} -------".format(json_file))
                continue

            # for every key
            for k in available_keys:
                for ele in temp_dict.get(k, []):
                    # element will be a dict
                    if ele.get("uuid", None) is None:
                        # only assign uuids to new templates
                        ele["uuid"] = "{}_{}".format(k, str(uuid4()))

            with open("./bank_data/"+json_file, "w+") as ff:
                ff.write(json.dumps(temp_dict, indent=4))