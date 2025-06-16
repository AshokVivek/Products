import boto3
import re
import sys

def create_client(region: str, client_type: str):
    client = boto3.client(client_type, region_name=region)
    return client

def create_parameter_map(list1: list[str], list2: list[str], count: int) -> dict:
    
    parameter_map: dict = {}
    for j in range(count):
        parameter_map[list1[j]] = list2[j]

    return parameter_map

def get_ssm_parameter_values(boto_ssm_client, ssm_parameters: list[str], parameter_map: dict, whitespace_delimiter: str) -> str:
    response = boto_ssm_client.get_parameters(
        Names=ssm_parameters,
        WithDecryption=True
    )

    result: str = ''
    ssm_parameters_and_values_in_response: list[str] = response.get('Parameters', [])
    
    for p in ssm_parameters_and_values_in_response:

        # replacing all occurences of whitespaces with the delimiter sent from the makefile.
        # # this is to ensure that whitespaces don't cause an issue while parsing values
        updated_value = re.sub(r'\s+', whitespace_delimiter, p.get('Value'))
        result += f"--param=\"{parameter_map.get(p.get('Name'))}={updated_value}\" "

    return result

if __name__ == "__main__":
    
    try:
        # Read arguments from command line
        lowercase_stage: str = sys.argv[1]
        lowercase_region: str = sys.argv[2]
        whitespace_delimiter: str = sys.argv[3]
        ssm_parameter_count: int = int(sys.argv[4])
        ssm_parameters_as_list: list[str] = sys.argv[5:5 + ssm_parameter_count]
        serverless_parameters_as_list: list[str] = sys.argv[5 + ssm_parameter_count:]
        ssm_parameter_count = len(ssm_parameters_as_list)

        if ssm_parameter_count != len(serverless_parameters_as_list):
            print("NA")

        else:
            # passing count to avoid a linear traversal
            parameter_map: dict = create_parameter_map(list1=ssm_parameters_as_list, list2=serverless_parameters_as_list, count=ssm_parameter_count)

            # initialize ssm boto client
            boto_ssm_client = create_client(client_type='ssm', region=lowercase_region)

            # decide on the batch count
            BATCH_SIZE: int = 10
            batch_count: int = int(ssm_parameter_count/ BATCH_SIZE)

            if ssm_parameter_count % BATCH_SIZE != 0:
                batch_count += 1

            # creating batches + invoking aws getparameter for all parameters + collating individual stringified results
            collated_result: str = ''
            for i in range(batch_count):
                ssm_parameters_to_be_considered = ssm_parameters_as_list[i * BATCH_SIZE: (i + 1) * BATCH_SIZE]
                collated_result += get_ssm_parameter_values(boto_ssm_client, ssm_parameters_to_be_considered, parameter_map, whitespace_delimiter)

            # value consumed as-is by Makefile             
            print(collated_result)

    except Exception as exception:
        print(str(exception))