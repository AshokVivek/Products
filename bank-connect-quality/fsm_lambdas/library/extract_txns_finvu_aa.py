import warnings
import os
import pandas as pd
from library.transaction_description import get_transaction_description
from library.transaction_channel import get_transaction_channel
from library.utils import add_hash_to_transactions_df


warnings.simplefilter(action = "ignore", category = FutureWarning)
pd.options.mode.chained_assignment = None

IS_SERVER = os.environ.get('IS_SERVER', False) in ["true", "1", "t"]


def get_transaction_channel_description_hash(transactions_list, bank, name, country='IN', account_category=''):
    
    # Ensuring final input is a dataframe along with storing return type as list or dataframe
    return_list = True
    if isinstance(transactions_list, list):
        transactions_df = pd.DataFrame(transactions_list)
    else:
        transactions_df = transactions_list
        return_list = False

    # adding transaction channel to the df
    # adding merchant category to df
    # adding unclean merchant to df
    transactions_df = get_transaction_channel(transactions_df, bank, country, account_category) 
    
    # adding description to df
    # adding mechant to df
    # adding is_lender to df
    transactions_df = get_transaction_description(transactions_df, name)

    # adding hash to transactions
    transactions_df = add_hash_to_transactions_df(transactions_df)

    
    # Ensuring return type as list or dataframe based on input type
    if return_list:
        return transactions_df.to_dict('records')
    else:
        return transactions_df