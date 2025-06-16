import os
import json
from library.utils import get_compiled_regex_list
from python.configs import DJANGO_BASE_URL, API_KEY, IS_SERVER
from python.api_utils import call_api_with_session


HEADERS = {
        'x-api-key': API_KEY
    }


def get_description_from_server(country="IN"):
    url = DJANGO_BASE_URL + f"/bank-connect/v1/internal/get_enrichment_regexes/?type=description&country={country}"
    response = call_api_with_session(url,"GET", None, HEADERS).json()
    return response

def get_everything_from_server(bank_name, country="IN"):
    url = DJANGO_BASE_URL + f"/bank-connect/v1/internal/get_enrichment_regexes/?type=all&bank_name={bank_name}&country={country}"
    response = call_api_with_session(url,"GET", None, HEADERS).json()
    return response

# lender, lender_small_words and loan_words in all the 3 lists put new words in lower case with no whitespace
def get_lender_list_from_the_server(country="IN"):
    print("getting lender list from the server")
    url = DJANGO_BASE_URL + f"/bank-connect/v1/internal/get_enrichment_regexes/?type=lender_list&country={country}"
    response = call_api_with_session(url,"GET", None, HEADERS).json()
    return response.get("lenders", [])

def get_merchant_category_from_server(country="IN"):
    url = DJANGO_BASE_URL + f"/bank-connect/v1/internal/get_enrichment_regexes/?type=merchant_category&country={country}"
    response = call_api_with_session(url,"GET", None, HEADERS).json()
    return response

def get_description_regexes(country="IN"):
    # converting country to uppercase as a safety check
    country = country.upper()

    # first check if description for this country is cached and kept in /tmp
    tmp_file_path = f"/tmp/description_{country}.json"
    if os.path.exists(tmp_file_path):
        data = json.load(open(tmp_file_path))
        return data
    
    description_data = get_description_from_server(country)

    # caching the data in the /tmp file
    with open(tmp_file_path, "w") as outfile:
        outfile.write(json.dumps(description_data, indent=4))
        print(f"cached description for {country} in {tmp_file_path}")
    
    return description_data

def get_unclean_merchant(bank_name, country="IN"):
    # converting country to uppercase as a safety check
    country = country.upper()

    # check if the data is present in the /tmp folder 
    tmp_file_path = f"/tmp/unclean_merchant_{bank_name}_{country}.json"
    if os.path.exists(tmp_file_path):
        final_data = json.load(open(tmp_file_path))
    else:
        url = DJANGO_BASE_URL + f"/bank-connect/v1/internal/get_enrichment_regexes/?type=unclean_merchant&bank_name={bank_name}&country={country}"
        response = call_api_with_session(url,"GET", None, HEADERS).json()
        
        merchant_debit_regex_list = response.get("debit")
        merchant_credit_regex_list = response.get("credit")

        final_data = {
            "merchant_debit_regex_list": merchant_debit_regex_list,
            "merchant_credit_regex_list": merchant_credit_regex_list
        }
        
        # cache this data in /tmp folder
        with open(tmp_file_path, "w") as outfile:
            outfile.write(json.dumps(final_data, indent=4))
            print(f"cached unclean merchant for {bank_name} and country {country} in {tmp_file_path}")
    
    # get compiled regexes for the files
    final_data["merchant_debit_regex_list"] = get_compiled_regex_list(final_data["merchant_debit_regex_list"])
    final_data["merchant_credit_regex_list"] = get_compiled_regex_list(final_data["merchant_credit_regex_list"])
    return final_data

def get_transaction_channel_lists(bank_name, country="IN"):
    # check if the data is present in the /tmp folder 
    tmp_file_path = f"/tmp/transaction_channel_{bank_name}_{country}.json"

    if os.path.exists(tmp_file_path):
        final_data = json.load(open(tmp_file_path))
    else:
        url = DJANGO_BASE_URL + f"/bank-connect/v1/internal/get_enrichment_regexes/?type=transaction_channel&bank_name={bank_name}&country={country}"
        response = call_api_with_session(url,"GET", None, HEADERS).json()
        
        # now preparing into a presentable format so that this conforms to the old method.
        debit_channel_dict = response.get("debit", {})
        debit_priority_order = list(debit_channel_dict.keys())
        credit_channel_dict = response.get("credit", {})
        credit_priority_order = list(credit_channel_dict.keys())

        final_data = {
            "debit_channel_dict" : debit_channel_dict,
            "debit_priority_order" : debit_priority_order,
            "credit_channel_dict" : credit_channel_dict,
            "credit_priority_order" : credit_priority_order
        }

        # cache this data in /tmp folder
        with open(tmp_file_path, "w") as outfile:
            outfile.write(json.dumps(final_data, indent=4))
            print(f"cached transaction channel for {bank_name} and country {country} in {tmp_file_path}")

    # get compiled regexes for the files
    for key in final_data["debit_channel_dict"]:
        final_data["debit_channel_dict"][key] = get_compiled_regex_list(final_data["debit_channel_dict"][key])

    for key in final_data["credit_channel_dict"]:
        final_data["credit_channel_dict"][key] = get_compiled_regex_list(final_data["credit_channel_dict"][key])

    return final_data

def get_lender_list(country="IN"):
    # first check if the lender_list for this country exists in the /tmp folder
    tmp_file_path = f"/tmp/lender_list_{country}.json"
    if os.path.exists(tmp_file_path):
        data = json.load(open(tmp_file_path))
        return data

    # get the data from the server
    categories = get_lender_list_from_the_server(country)
    
    # caching the data in the /tmp file
    with open(tmp_file_path, "w") as outfile:
        outfile.write(json.dumps(categories, indent=4))
        print(f"cached lender list for {country} in {tmp_file_path}")
    return categories

def get_merchant_category_data(country="IN"):
    # first check if merchant category for this country is cached and kept in /tmp
    tmp_file_path = f"/tmp/merchant_category_{country}.json"
    if os.path.exists(tmp_file_path):
        data = json.load(open(tmp_file_path))
        return data
    
    # get the data from the server
    categories = get_merchant_category_from_server(country)

    # caching the data in the /tmp file
    with open(tmp_file_path, "w") as outfile:
        outfile.write(json.dumps(categories, indent=4))
        print(f"cached merchant category for {country} in {tmp_file_path}")
    return categories

def check_and_get_everything(bank_name, country="IN"):
    if IS_SERVER:
        return

    print(f"Trying to get the data for bank_name : {bank_name} and country: {country}")
    files_to_check = [f"/tmp/merchant_category_{country}.json", f"/tmp/lender_list_{country}.json", f"/tmp/lender_regex_list_{country}.json", f"/tmp/transaction_channel_{bank_name}_{country}.json", f"/tmp/unclean_merchant_{bank_name}_{country}.json", f"/tmp/description_{country}.json"]
    
    all_present = True
    for items in files_to_check:
        if not os.path.exists(items):
            all_present = False
            break
    
    if all_present:
        print("Everything that is needed, is here. No need to get anymore.")
        return
    
    print("Getting all from the server, and caching everything in /tmp")

    if not all_present:
        # calling the api, getting the data and caching everything
        data_from_the_server = get_everything_from_server(bank_name, country)
    
    with open(f"/tmp/merchant_category_{country}.json", "w") as outfile:
        outfile.write(json.dumps(data_from_the_server.get('merchant_category', {}), indent=4))
        print(f"cached merchant category for {country}")
    
    with open(f"/tmp/lender_list_{country}.json", "w") as outfile:
        outfile.write(json.dumps(data_from_the_server.get('lender_list', {}).get('lenders', []), indent=4))
        print(f"cached lender list for {country}")

    with open(f"/tmp/lender_regex_list_{country}.json", "w") as outfile:
        outfile.write(json.dumps(data_from_the_server.get('lender_regex_list', []), indent=4))
        print(f"cached lender regex list for {country}")
    
    transaction_channel_data = data_from_the_server.get('transaction_channel', {})
    debit_channel_dict = transaction_channel_data.get("debit", {})
    debit_priority_order = list(debit_channel_dict.keys())
    credit_channel_dict = transaction_channel_data.get("credit", {})
    credit_priority_order = list(credit_channel_dict.keys())

    final_data = {
        "debit_channel_dict" : debit_channel_dict,
        "debit_priority_order" : debit_priority_order,
        "credit_channel_dict" : credit_channel_dict,
        "credit_priority_order" : credit_priority_order
    }

    with open(f"/tmp/transaction_channel_{bank_name}_{country}.json", "w") as outfile:
        outfile.write(json.dumps(final_data, indent=4))
        print(f"cached transaction channel for {bank_name} and country {country}")
    
    unclean_merchant_regex_data = data_from_the_server.get('unclean_merchant')
    merchant_debit_regex_list = unclean_merchant_regex_data.get("debit")
    merchant_credit_regex_list = unclean_merchant_regex_data.get("credit")

    final_data = {
        "merchant_debit_regex_list": merchant_debit_regex_list,
        "merchant_credit_regex_list": merchant_credit_regex_list
    }
    
    with open(f"/tmp/unclean_merchant_{bank_name}_{country}.json", "w") as outfile:
        outfile.write(json.dumps(final_data, indent=4))
        print(f"cached unclean merchant for {bank_name} and country {country}")
    
    with open(f"/tmp/description_{country}.json", "w") as outfile:
        outfile.write(json.dumps(data_from_the_server.get('description'), indent=4))
        print(f"cached description for {country}")