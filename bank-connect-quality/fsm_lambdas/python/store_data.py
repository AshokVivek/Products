import json

def store_data_from_enrichment_regexes(enrichment_regexes, bank_name, country="IN"):

    if len(enrichment_regexes.keys())==0:
        return
    
    keys_needed = ['merchant_category' ,'transaction_channel' ,'unclean_merchant' ,'lender_list', 'lender_regex_list' ,'description']
    
    if sorted(keys_needed)!=sorted(enrichment_regexes.keys()):
        return

    print("regexes retrieved from the server, using this to populate the files")

    with open(f"/tmp/merchant_category_{country}.json", "w") as outfile:
        outfile.write(json.dumps(enrichment_regexes.get('merchant_category', {}), indent=4))
        print(f"cached merchant category for {country}")
    
    with open(f"/tmp/lender_list_{country}.json", "w") as outfile:
        outfile.write(json.dumps(enrichment_regexes.get('lender_list', {}).get("lenders", []), indent=4))
        print(f"cached lender list for {country}")

    with open(f"/tmp/lender_regex_list_{country}.json", "w") as outfile:
        outfile.write(json.dumps(enrichment_regexes.get('lender_regex_list', []), indent=4))
        print(f"cached lender regex list for {country}")
    
    transaction_channel_data = enrichment_regexes.get('transaction_channel')
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
    
    unclean_merchant_regex_data = enrichment_regexes.get('unclean_merchant')
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
        outfile.write(json.dumps(enrichment_regexes.get('description'), indent=4))
        print(f"cached description for {country}")