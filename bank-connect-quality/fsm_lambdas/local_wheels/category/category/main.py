from sentry_sdk import capture_exception
from .utils import categorise_lender_transaction_single_category, categorise_mutual_funds, categories_data
from copy import deepcopy
import warnings
import pandas as pd
from python.api_utils import call_api_with_session
import json


warnings.simplefilter(action = "ignore", category = FutureWarning)
pd.options.mode.chained_assignment = None


class SingleCategory:
    def __init__(self, bank_name, transactions, categorize_server_ip=None):
        print(f"Initialized single category with {len(transactions)}")
        self.bank_name = bank_name
        self.transactions = transactions
        self.categorize_server_ip = categorize_server_ip
        
        self.txn_cats = pd.DataFrame(categories_data)
        self.txn_cats.fillna("", inplace=True)
    
    def categorize_from_rust_server(self):
        url = self.categorize_server_ip + "/categorize"
        payload = json.dumps({
            "bank_name": self.bank_name,
            "transactions": self.transactions
        }, default=str)
        headers = {
            'Content-Type': 'application/json'
        }

        transactions = deepcopy(self.transactions)
        try:
            response = call_api_with_session(url,"POST", payload, headers, timeout=5).json()
            transactions = response["transactions"]
            
            # merge the two dictionaries
            common_keys = ["hash", "transaction_note"]
            og_transactions_map = {}
            new_transactions_map = {}
            for txn in self.transactions:
                key = tuple(txn[i] for i in common_keys)
                og_transactions_map[key] = txn
            for txn in transactions:
                key = tuple(txn[i] for i in common_keys)
                new_transactions_map[key] = txn
            
            final_transactions = []
            for tup_key, d1 in og_transactions_map.items():
                d2 = new_transactions_map.get(tup_key, {})
                if d2:
                    final_transactions.append({**d1, **d2})
                else:
                    print(tup_key, " missing from category_transactions")
                    raise Exception(tup_key, " missing from category_transactions")
            transactions = final_transactions
        except Exception as e:
            capture_exception(e)
            print(e)
        return transactions

    def categorize_from_forward_mapper(self):
        records = self.transactions
        txn_cats = self.txn_cats
        
        for transaction in records:
            merchant_category = transaction["merchant_category"].strip()
            transaction_channel = transaction["transaction_channel"].strip()
            description = transaction["description"].strip()
            transaction_type = transaction["transaction_type"]
            transaction["category"] = ""
            filtered = txn_cats[
                (txn_cats.transaction_channel==transaction_channel) & 
                (txn_cats.description==description) &  
                (transaction_channel not in [None, ""]) & 
                (description not in [None, ""])
            ]
            if len(filtered) > 0:
                filtered = filtered['category'].to_list()
                transaction["category"] = filtered[0] if filtered else None
                if description == "lender_transaction":
                    transaction = categorise_lender_transaction_single_category(transaction)
                if transaction_channel == "self_transfer" or description == "self_transfer":
                    continue
                continue
            
            # check for just transaction channel match
            filtered = txn_cats[(txn_cats.transaction_channel==transaction_channel) & (transaction_channel not in [None, ""])]
            if len(filtered) > 0:
                filtered = filtered['category'].to_list()
                transaction["category"] = filtered[0] if filtered else None
                if transaction_channel == "self_transfer":
                    continue
                continue
            
            # check for just description match
            filtered = txn_cats[(txn_cats.description==description) & (description not in [None, ""])]
            if len(filtered) > 0:
                filtered = filtered['category'].to_list()
                transaction["category"] = filtered[0] if filtered else None
                if description == "lender_transaction":
                    transaction = categorise_lender_transaction_single_category(transaction)
                if description == "mutual_funds":
                    transaction = categorise_mutual_funds(transaction)
                if description in ["investments", "trading/investments"]:
                    transaction["category"] = "Investment Income" if transaction_type == "credit" else "Investment Expense"
                if description == "self_transfer":
                    continue
                continue

            filtered_merchant_category = txn_cats[(txn_cats.merchant_category==merchant_category) & (merchant_category not in [None, ""])]
            if len(filtered_merchant_category) > 0:
                filtered_mc_data = filtered_merchant_category["category"].to_list()
                transaction["category"] = filtered_mc_data[0] if filtered_mc_data else None
                if merchant_category == "mutual_funds":
                    transaction = categorise_mutual_funds(transaction)
                if description == "lender_transaction":
                    transaction = categorise_lender_transaction_single_category(transaction)
                if merchant_category in ["investments", "trading/investments"]:
                    transaction["category"] = "Investment Income" if transaction_type == "credit" else "Investment Expense"
                continue

            if transaction_channel == "self_transfer" or description == "self_transfer":
                transaction["category"] = "Self Transfer"

            if "transfer" in description.lower():
                transaction["category"] = description
                continue

            transaction["category"] = "Others"
        return records