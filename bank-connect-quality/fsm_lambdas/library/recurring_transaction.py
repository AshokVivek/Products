import re
import warnings
import pandas as pd
import polars as pl
import numpy as np
from datasketch import MinHash, MinHashLSH
from rapidfuzz import fuzz as rfuzz
from rapidfuzz import process
from library.utils import match_compiled_regex
from library.helpers.constants import TRANSACTION_CHANNELS


warnings.simplefilter(action = "ignore", category = FutureWarning)
pd.options.mode.chained_assignment = None


month_year_regex = re.compile(
    r"(jan|feb|mar|apr|may|jun|june|jul|july|aug|sep|oct|nov|dec)([0-9]{2,4})"
)

# 'chq','paid','dep','cash',
words_to_remove = (['imps', 'trf', 'inet', 'cms', 'fund', 'upi', 'rtgs', 'neft', 'mmt', 'pos', 'ecom', 'pur', 'atm',
                   'cash', 'p2a', 'by', 'clg', 'p2m', 'refund', 'balance', 'date', 'value', 'transfer', 'withdrawl',
                   'debit', 'month', 'january', 'february', 'march', 'april', 'may', 'june', 'july', 'august',
                   'september', 'october', 'november', 'december', 'jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul',
                   'aug', 'sep', 'oct', 'nov', 'dec', 'mpay', 'trtr', 'cms',
                   'inward','cts','bank','hdfc','citi','icici','axis',   #new addition start from here
                   'payment','phone','ph','paytm','phonepe','bharatpe','from','hdfc','paym','payme','bkid','pay',
                    'ubin','punb','phonep','yesb','pytm','sbin','axmb','utib','ubin','dbssin','deutbgl','ibkl',
                    'ioba','icicsf','inf','inft','federal','fdrl','central','proc','inetimps','branch','money','received','transaction','vpa'
                    'using','reference','number','from', 'sent', 'id', 'to'])

words_to_remove_lender_recurring = [
    "imps", "trf", "inet", "cms", "fund", "upi", "rtgs", "neft", "mmt", "pos", "ecom", "pur", "atm",
    "cash", "p2a", "balance", "date", "value", "transfer", "withdrawl", "bank", "debit", "month", 
    "january", "february", "march", "april", "may", "june", "july", "august", "september", "october", "november", "december",
    "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec",
    "mpay", "trtr", "cms", "inward", "cts", "payment", "phone", "from", "paym", "payme", "pay", "inetimps",
    "vpa", "using", "reference", "number", "from", "sent", "id", "to",
]

words_in_upi_id = (['ybl','paytm','payt','ok','upi','barb','pu','okax','okaxis','ind','abfs','axl',
                    'idfcbank','axisb','oksbi','okbizaxis','icici','okhdfc','okicici','axisbank','y'])

split = [' ', '/', ':', '-', ',', ';', '.']
jaccard_split = split + ['@']

acc_regx = re.compile('.*?([0-9]*[Xx\\.\\*]+[0-9]+[xX\\.\\*]*).*')
num_regex = re.compile('.*?([0-9]+).*')

#number of transaction after which transaction calculation starts using jaccard logic
JACCARD_CALCULATION_LIMIT=1000

def get_credit_recurring_transactions(df, use_workers=False):
    df = pl.DataFrame(df)
    not_allowed_list = [TRANSACTION_CHANNELS.REVERSAL, TRANSACTION_CHANNELS.REFUND, 'cash_withdrawl', 'international_transaction_arbitrage',
                        'bank_interest', 'inward_cheque_bounce', 'investment_cashin', 'bank_charge',
                        'outward_cheque_bounce', 'self_transfer']
    pattern = '(RENT|FAMILY)'
    allowed_df = df.filter(~pl.col('transaction_channel').is_in(not_allowed_list))
    allowed_df = df.filter(~pl.col('transaction_note').str.contains(f"(?i){pattern}"))
    if allowed_df.shape[0] > 0:
        allowed_df = allowed_df.unique(subset=['hash'])
        credit_df = allowed_df.filter(pl.col('transaction_type') == 'credit')

        if credit_df.shape[0] > JACCARD_CALCULATION_LIMIT:
            credit_df = credit_df.with_columns(
                pl.struct(credit_df.columns).map_elements(lambda x: clean_unclean_merchant(x, ""), return_dtype=pl.Utf8).alias("clean_transaction_note")
            )
            credit_df = credit_df.filter(
                pl.col('clean_transaction_note').str.len_chars() > 2
            )

            credit_df = credit_df.sort(
                by=['transaction_channel', 'clean_transaction_note'],
                descending=[True, True]
            )
            credit_recurring_list = jaccard_similarity(credit_df)
        else:
            credit_df = credit_df.with_columns(
                pl.struct(credit_df.columns).map_elements(lambda x: clean_transaction_note(x, ""), return_dtype=pl.Utf8).alias("clean_transaction_note")
            )

            credit_df = credit_df.sort(
                by=['transaction_channel', 'clean_transaction_note'],
                descending=[True, True]
            )
            credit_recurring_list = get_recurring(credit_df, use_workers)
        credit_recurring_list = sorted(
            credit_recurring_list, key=len, reverse=True)
        return credit_recurring_list
    else:
        return []

def get_all_transaction_fuzz_score(df, transaction1, fuzz_threshold):
    df = pl.DataFrame(df)
    df = df.with_columns(
        pl.struct(df.columns).map_elements(lambda x: clean_transaction_note(x, ""), return_dtype=pl.Utf8).alias("clean_transaction_note")
    )
    df = df.sort(by="date", descending=False)
    transaction_list = df.to_dicts()
    final_matched_list = []

    for i_counter in range(len(transaction_list)):
        each_transaction = transaction_list[i_counter]

        fuzz_partial_score = calculate_fuzzy_scores(each_transaction["clean_transaction_note"], transaction1["clean_transaction_note"])

        if fuzz_partial_score > fuzz_threshold:
            final_matched_list.append(each_transaction)

    return final_matched_list


def get_debit_recurring_transaction(df, use_workers=False):
    not_allowed_list = [TRANSACTION_CHANNELS.REVERSAL, TRANSACTION_CHANNELS.REFUND, 'cash_deposit', 'cash_withdrawl', 'international_transaction_arbitrage',
                        'bank_interest', 'inward_cheque_bounce', 'investment_cashin', 'bank_charge',
                        'outward_cheque_bounce']
    allowed_df = df.filter(~pl.col('transaction_channel').is_in(not_allowed_list))
    if allowed_df.shape[0] > 0:
        allowed_df = allowed_df.unique(subset=['hash'])
        debit_df = allowed_df.filter(pl.col('transaction_type')=='debit')

        if debit_df.shape[0] > JACCARD_CALCULATION_LIMIT:
            debit_df = debit_df.with_columns(
                pl.struct(debit_df.columns).map_elements(lambda x: clean_unclean_merchant(x, ""), return_dtype=pl.Utf8).alias("clean_transaction_note")
            )

            debit_df = debit_df.filter(
                pl.col('clean_transaction_note').str.len_chars() > 2
            )

            debit_df = debit_df.sort(
                by=['transaction_channel', 'clean_transaction_note'],
                descending=[True, True]
            )
            debit_recurring_list = jaccard_similarity(debit_df)
        else:
            debit_df = debit_df.with_columns(
                pl.struct(debit_df.columns).map_elements(lambda x: clean_transaction_note(x, ""), return_dtype=pl.Utf8).alias("clean_transaction_note")
            ) 

            debit_df = debit_df.sort(
                by=['transaction_channel', 'clean_transaction_note'],
                descending=[True, True]
            )

            debit_recurring_list = get_recurring(debit_df, use_workers)
        debit_recurring_list = sorted(
            debit_recurring_list, key=len, reverse=True)
        return debit_recurring_list
    else:
        return []

def get_unclean_merchant_grouped_transaction(df, cred_db):
    allowed_df = df
    if allowed_df.shape[0] > 0:
        allowed_df.drop_duplicates(subset=['hash'], inplace=True)
        txn_df = allowed_df[allowed_df['transaction_type'] == cred_db]
        txn_df.sort_values(
            by=['transaction_channel', 'unclean_merchant'], ascending=False, inplace=True)
        txn_df.reset_index(drop=True, inplace=True)
        recurring_list, other_list = get_unclean_merchant_grouping(txn_df, cred_db)
        recurring_list = sorted(
            recurring_list, key=len, reverse=True)
        recurring_list.append(other_list)
        return recurring_list
    else:
        return []

def get_all_unclean_merchant_grouped_transaction(df):
    df = pd.DataFrame(df)
    credit_recurring_list = get_unclean_merchant_grouped_transaction(df, 'credit')
    debit_recurring_list = get_unclean_merchant_grouped_transaction(df, 'debit')
    return credit_recurring_list, debit_recurring_list

def get_all_recurring_transaction(df):
    credit_recurring_list = get_credit_recurring_transactions(df)
    debit_recurring_list = get_debit_recurring_transaction(df)
    return credit_recurring_list, debit_recurring_list


def clean_transaction_note(transaction_row, recurring_type=''):
    if recurring_type=="lender":
        WORDS_TO_REMOVE_FINAL = words_to_remove_lender_recurring
    else:
        WORDS_TO_REMOVE_FINAL = words_to_remove
    transaction_note = transaction_row['transaction_note']
    transaction_channel = transaction_row['transaction_channel']
    if isinstance(transaction_note, str):
        transaction_note = transaction_note.lower()
        if recurring_type == "lender":
            transaction_note = re.sub(month_year_regex, ' ', transaction_note)
        transaction_note_list = multiple_split(transaction_note, split)
        final_word_list = []
        if transaction_channel != 'upi':
            for each_word in transaction_note_list:
                if each_word in words_to_remove:
                    each_word=''

                #capturing account number
                captured_acc = match_compiled_regex(each_word, acc_regx, 1)
                if captured_acc is None:
                    each_word = re.sub('\\d', '', each_word)
                else:
                    each_word = re.sub('x','',each_word)  #removing x from account number for eg. xxxxxxxxxxx1152 to 1152

                if len(each_word) < 3:
                    each_word = ''
                if each_word in words_to_remove:
                    each_word = ''
                final_word_list.append(each_word)
            clean_note = ' '.join(final_word_list)
            clean_note = re.sub('\\s+', ' ', clean_note).strip()
            return clean_note
        else:
            clean_note = get_clean_note_upi(transaction_note_list, WORDS_TO_REMOVE_FINAL)
            return clean_note
    else:
        return ''


def multiple_split(s, seps):
    res = [s]
    for sep in seps:
        s, res = res, []
        for seq in s:
            res += seq.split(sep)
    return res

def digits_in_string(string):
    cnt=0
    for s in string:
        if s.isdigit():
            cnt+=1
    return cnt

def get_clean_note_upi(transaction_note_list, WORDS_TO_REMOVE_FINAL):
    
    final_word_list = []
    for each_word in transaction_note_list:
        # for removable_word in words_to_remove:
        #     each_word = each_word.replace(removable_word, '')
        if each_word in WORDS_TO_REMOVE_FINAL:
            each_word = '' 

        #check for upi id
        is_upi_id=False
        if '@' in each_word:
            split_word_list=each_word.split('@')
            if split_word_list[1] in words_in_upi_id:
                is_upi_id = True
                each_word=split_word_list[0]

        #check for phone number
        captured_num = match_compiled_regex(each_word, num_regex, 1)
        is_phone_number=False
        if is_upi_id is False and captured_num is not None:
            if len(captured_num) != 10:   
                is_phone_number=False
                each_word = each_word.replace(captured_num, '')
            else: is_phone_number = True

        #removing random words
        digits=digits_in_string(each_word)
        if is_upi_id is False and is_phone_number is False and digits >= 2:
            each_word=''

        if each_word in WORDS_TO_REMOVE_FINAL:
            each_word = '' 

        if len(each_word) < 3:
            each_word = ''
        final_word_list.append(each_word)
    clean_note = ' '.join(final_word_list)
    clean_note = re.sub('\\s+', ' ', clean_note).strip()
    return clean_note


def get_recurring_using_dict_with_fuzz_threshold(df, threshold=90):
    df = pl.DataFrame(df)
    df = df.sort(by="date", descending=False)
    df = df.with_columns(
        pl.struct(df.columns).map_elements(lambda x: clean_transaction_note(x, ""), return_dtype=pl.Utf8).alias("clean_transaction_note")
    )

    transaction_list = df.to_dicts()
    already_matched_list = set()
    final_matched_list = []

    for i_counter in range(len(transaction_list)):
        each_transaction = transaction_list[i_counter]
        match_list = []
        if each_transaction["hash"] not in already_matched_list:
            match_list.append(each_transaction)
            already_matched_list.add(each_transaction["hash"])
            for j_counter in range(i_counter + 1, len(transaction_list)):
                each_transaction_2 = transaction_list[j_counter]
                transaction_channel_1 = each_transaction["transaction_channel"]
                transaction_channel_2 = each_transaction_2["transaction_channel"]
                condition_0 = (transaction_channel_1 == transaction_channel_2) or (
                    transaction_channel_1 == "salary" or transaction_channel_2 == "salary"
                )

                condition_1 = each_transaction_2["hash"] not in already_matched_list

                if condition_0 and condition_1:
                    fuzz_partial_score = calculate_fuzzy_scores(
                        each_transaction["clean_transaction_note"], each_transaction_2["clean_transaction_note"]
                    )

                    if condition_0 and condition_1 and fuzz_partial_score >= threshold:
                        match_list.append(each_transaction_2)

                        already_matched_list.add(each_transaction_2["hash"])
        if len(match_list) > 1:
            final_matched_list.append(match_list)
    return final_matched_list


def get_recurring_using_dict(df):
    df = df.sort(by='date', descending=False)
    transaction_list = df.to_dicts()
    already_matched_list = set()
    final_matched_list = []

    for i_counter in range(len(transaction_list)):
        each_transaction = transaction_list[i_counter]
        match_list = []
        if each_transaction['hash'] not in already_matched_list:
            match_list.append(each_transaction)
            already_matched_list.add(each_transaction['hash'])
            for j_counter in range(i_counter+1, len(transaction_list)):
                each_transaction_2 = transaction_list[j_counter]
                transaction_channel_1 = each_transaction['transaction_channel']
                transaction_channel_2 = each_transaction_2['transaction_channel']
                merchant_1 = str(each_transaction.get('unclean_merchant'))
                merchant_2 = str(each_transaction_2.get('unclean_merchant'))
                condition_0 = (transaction_channel_1 == transaction_channel_2) or (
                    transaction_channel_1 == 'salary' or transaction_channel_2 == 'salary')

                condition_1 = (
                    each_transaction_2['hash'] not in already_matched_list)

                condition_note_len = (abs(len(each_transaction['clean_transaction_note']) - len(
                    each_transaction_2['clean_transaction_note']) < 10))

                condition_merchan_len = (
                    len(merchant_1) > 10 and len(merchant_2) > 10)

                if condition_0 and condition_1 and (condition_note_len or condition_merchan_len):
                    fuzz_score_1 = rfuzz.partial_token_sort_ratio(each_transaction['clean_transaction_note'],
                                                                 each_transaction_2['clean_transaction_note'])

                    fuzz_score_2 = rfuzz.ratio(each_transaction['clean_transaction_note'],
                                              each_transaction_2['clean_transaction_note'])

                    fuzz_score_3 = rfuzz.token_set_ratio(each_transaction['clean_transaction_note'],
                                                        each_transaction_2['clean_transaction_note'])

                    fuzz_partial_score = max(
                        fuzz_score_1, fuzz_score_2, fuzz_score_3)

                    length_1 = len(each_transaction['clean_transaction_note'])
                    length_2 = len(
                        each_transaction_2['clean_transaction_note'])

                    condition_2 = (fuzz_partial_score > 93)

                    condition_3 = (abs(length_1 - length_2) < 10)

                    condition_2_1 = (fuzz_partial_score > 73)

                    condition_3_1 = (length_1 > 35 and length_2 > 35)

                    condition_2_2 = (fuzz_partial_score > 60)

                    condition_3_2 = (length_1 > 50 and length_2 > 50)

                    condition_2_3 = (fuzz_partial_score > 87)

                    condition_3_3 = (length_1 > 10 and length_2 > 10)

                    condition_2_4 = (fuzz_partial_score >= 78)

                    condition_3_4 = (length_1 >= 20 and length_2 >= 20)

                    condition_2_5 = (rfuzz.WRatio(
                        merchant_1, merchant_2) >= 90)

                    condition_3_5 = (len(merchant_1) >=
                                     8 and len(merchant_2) >= 8)

                    condition_4 = (length_1 > 2 and length_2 > 2)

                    # print each_transaction_2['clean_transaction_note'], each_transaction['clean_transaction_note']
                    #
                    # print each_transaction_2['amount'], each_transaction['amount']
                    #
                    # print condition_0, condition_1, (condition_2 and condition_3), (condition_2_1 and condition_3_1), (
                    #     condition_2_2 and condition_3_2)

                    if condition_0 and condition_1 and (
                            (condition_2 and condition_3) or (condition_2_1 and condition_3_1) or (
                            condition_2_2 and condition_3_2) or (condition_2_3 and condition_3_3) or (condition_2_4 and condition_3_4) or (condition_2_5 and condition_3_5)) and condition_4:
                        match_list.append(each_transaction_2)
                        # print "MATCHED"
                        # print each_transaction_2['clean_transaction_note'], each_transaction['clean_transaction_note']
                        already_matched_list.add(each_transaction_2['hash'])
                    # else:
                    #     break
                # elif (condition_0 == False) and (condition_1 == True):
                #     break
        if len(match_list) > 1:
            final_matched_list.append(match_list)
    return final_matched_list

def clean_unclean_merchant(row, recurring_type):
    unclean_merchant = row['unclean_merchant']
    if unclean_merchant is None or unclean_merchant == '':
        return clean_transaction_note(row, recurring_type)
    unclean_merchant_list = multiple_split(unclean_merchant, jaccard_split)
    final_word_list = []
    for each_word in unclean_merchant_list:
        if each_word in words_to_remove:
            each_word = ''
        elif digits_in_string(each_word) != 10:
            each_word = ''
        else:
            final_word_list.append(each_word)
    clean_merchant = ' '.join(final_word_list)
    clean_merchant = re.sub('\\s+', ' ', clean_merchant).strip()
    return clean_merchant

def get_ngrams(string, n=3):
    return {string[i:i+n] for i in range(len(string) - n + 1)}

def get_minhash(s, hash_values, permutations):
    m = MinHash(num_perm=128, permutations=permutations, hashvalues=hash_values)
    update_values = []
    for shingle in get_ngrams(s):
        update_values.append(shingle.encode('utf8'))
    m.update_batch(update_values)
    return m

def jaccard_similarity(df):
    lsh = MinHashLSH(threshold=0.6, num_perm=128)
    minhash_keys = {}
    tmp_minhash = MinHash(num_perm=128)
    hash_values = tmp_minhash.hashvalues
    permutations = tmp_minhash.permutations
    for i in range(len(df)):
        txn_note_minhash = get_minhash(df['clean_transaction_note'][i], hash_values, permutations)
        minhash_keys[df['clean_transaction_note'][i]] = txn_note_minhash
        lsh.insert(i, txn_note_minhash)
    
    groups = []
    already_matched_set = set()
    for i in range(len(df)):
        if i not in already_matched_set:
            txn_note_minhash = minhash_keys[df['clean_transaction_note'][i]]
            result = lsh.query(txn_note_minhash)
            for index in result:
                already_matched_set.add(index)
            if len(result)>1:
                groups.append(result)
    
    transaction_list = df.to_dicts()
    final_matched_list = []
    already_added_txns = set()
    for group in groups:
        match_list = []
        for index in group:
            if index not in already_added_txns:
                match_list.append(transaction_list[index])
                already_added_txns.add(index)
        if len(match_list)>1:
            final_matched_list.append(match_list)
    return final_matched_list

def get_recurring(df, use_workers=False):
    
    #for lambdas
    if not use_workers:
        return get_recurring_using_dict(df)

    df = df.sort(by='date', descending=False)
    transaction_list = df.to_dicts()
    already_matched_list = set()
    final_matched_list = []

    clean_transaction_note_list = df["clean_transaction_note"].to_list()
    unclean_merchant_list = df["unclean_merchant"].to_list()

    token_set_ratio_list = process.cdist(clean_transaction_note_list, clean_transaction_note_list, scorer=rfuzz.token_set_ratio,dtype=np.uint8, workers=4, score_cutoff=59)
    ratio_list = process.cdist(clean_transaction_note_list, clean_transaction_note_list, scorer=rfuzz.ratio,workers=4,dtype=np.uint8, score_cutoff=59)
    WRatio_list = process.cdist(unclean_merchant_list, unclean_merchant_list, scorer=rfuzz.WRatio,workers=4,dtype=np.uint8,score_cutoff=89)
    partial_token_sort_ratio = process.cdist(clean_transaction_note_list, clean_transaction_note_list, scorer=rfuzz.partial_token_sort_ratio,workers=4,dtype=np.uint8,score_cutoff=59)

    counter=-1
    for each_transaction in transaction_list:
        counter+=1
        match_list = []
        if each_transaction['hash'] not in already_matched_list:
            match_list.append(each_transaction)
            already_matched_list.add(each_transaction['hash'])

            tmp_df=df.with_columns(pl.Series("fuzz_score_1", token_set_ratio_list[counter]),
                pl.Series("fuzz_score_2", ratio_list[counter]),
                pl.Series("fuzz_score_3", partial_token_sort_ratio[counter]),
                pl.Series("wratio", WRatio_list[counter])
            )

            tmp_df = tmp_df.with_columns( 
                pl.when( (pl.col('transaction_channel')==each_transaction['transaction_channel']) | (pl.col('transaction_channel')=='salary') | (each_transaction['transaction_channel']=='salary'))
                                    .then(True).otherwise(False).alias('condition_0'),
                                    pl.when(~pl.col('hash').is_in(already_matched_list)).then(True).otherwise(False).alias('condition_1') ,
                                    pl.when( ( pl.col('clean_transaction_note').str.len_chars() - len(each_transaction['clean_transaction_note'])).abs() <10 ).then(True).otherwise(False).alias('condition_note_len') ,
                                    pl.when( (pl.col('unclean_merchant').str.len_chars() > 10) & (len(each_transaction['unclean_merchant']) > 10) ).then(True).otherwise(False).alias('condition_merchan_len')
                )

            tmp_df = tmp_df.filter( (pl.col('condition_0') &  pl.col('condition_1') ) & (pl.col('condition_note_len') | pl.col('condition_merchan_len')))

            tmp_df = tmp_df.with_columns(pl.max_horizontal(["fuzz_score_1", "fuzz_score_2", "fuzz_score_3"]).alias("fuzz_partial_score"),
                pl.col('clean_transaction_note').str.len_chars().alias('length_2'),
                pl.lit(len(each_transaction['clean_transaction_note'])).alias('length_1'),
            ).filter(
                ( pl.col('length_1') > 2) & (pl.col('length_2') > 2 ) &
                (  (pl.col('fuzz_partial_score')>93) & (  (pl.col('length_1')- pl.col('length_2')).abs()<10)) |
                ( (pl.col('fuzz_partial_score') > 73) & (pl.col('length_1')>35) & (pl.col('length_2')>35) ) |
                ( (pl.col('fuzz_partial_score') > 60) & (pl.col('length_1')>50) & (pl.col('length_2')>50) ) |
                ( (pl.col('fuzz_partial_score') > 87) & (pl.col('length_1')>10) & (pl.col('length_2')>10) ) |
                ( (pl.col('fuzz_partial_score') >= 78) & (pl.col('length_1')>=20) & (pl.col('length_2')>=20) ) |
                (  (pl.col('wratio')>93) & ( pl.col('unclean_merchant').str.len_chars() >= 8) & (len(each_transaction['unclean_merchant']) >=8 ) )
            )

            tmp_df = tmp_df.drop(["condition_0", "condition_1", "condition_note_len", "condition_merchan_len", "length_1",
                "length_2", "fuzz_score_1", "fuzz_score_2", "fuzz_score_3", 
                "fuzz_partial_score", "wratio"])

            hash_values = tmp_df["hash"].to_list()
            matches = tmp_df.to_dicts()
            match_list.extend(matches)
            if len(match_list) > 1:
                final_matched_list.append(matches)
                already_matched_list.update(hash_values)
    return final_matched_list

def get_unclean_merchant_grouping(df, cred_db):
    grouped_transactions = {}
    other_transaction_list = []
    for index,transaction in df.iterrows():
        unclean_merchant = transaction['unclean_merchant'].strip() # Remove extra spaces if any
        if unclean_merchant == '' or unclean_merchant is None:
            unclean_merchant = f'OTHER {cred_db.upper()}S'
            transaction['unclean_merchant'] = f'OTHER {cred_db.upper()}S'
            other_transaction_list.append(transaction)
            continue
        if unclean_merchant in grouped_transactions:
            grouped_transactions[unclean_merchant].append(transaction)
        else:
            grouped_transactions[unclean_merchant] = [transaction]
    
    grouped_transactions_list = [transactions_list for transactions_list in grouped_transactions.values()]
    return grouped_transactions_list, other_transaction_list

def get_parameters(recurring_list, tag='Source'):
    obj = {}
    result = pd.DataFrame(columns=[tag, 'Total Amount', 'Average Amount', 'Number of Transactions', 'Max Amount',
                                   'Average Balance at Transaction'])
    for each in range(0, len(recurring_list)):
        each_reoccurance = pd.DataFrame(recurring_list[each])
        obj['Total Amount'] = each_reoccurance['amount'].sum()
        obj['Average Amount'] = each_reoccurance['amount'].mean()
        obj['Number of Transactions'] = each_reoccurance['amount'].count()
        obj['Max Amount'] = each_reoccurance['amount'].max()
        if str(each_reoccurance['balance'].dtype) == 'object':
            each_reoccurance['balance'] = each_reoccurance['balance'].apply(lambda x: float(x.replace(",", "")))
        obj['Average Balance at Transaction'] = each_reoccurance['balance'].mean()
        column = {tag: tag + str(each + 1)}
        column.update(obj)
        row_labels=[0]
        df=pd.DataFrame(data = column,index = row_labels)
        result = pd.concat([result,df], ignore_index=True)
        result = result[
            [tag, 'Total Amount', 'Average Amount', 'Number of Transactions', 'Max Amount',
             'Average Balance at Transaction']]
    result = result.set_index(tag)
    result = result.astype(int)
    return result, recurring_list

def get_recurring_subgroups(df, amt_deviation=0.4, date_deviation=6):
    df.sort_values(by='date', inplace=True)
    transaction_list = df.to_dict('records')
    already_matched_list = set()
    final_matched_list = []
    for each_transaction in transaction_list:
        prev_matched_txn = None
        match_list = []
        if each_transaction['hash'] not in already_matched_list:
            match_list.append(each_transaction)
            already_matched_list.add(each_transaction['hash'])
            for each_transaction_2 in transaction_list:
                condition_1 = (
                    each_transaction_2['hash'] not in already_matched_list)

                if condition_1:
                    condition_amt_deviation = (
                        (each_transaction_2['amount'] <= each_transaction['amount'] * (1+amt_deviation)
                        ) and (each_transaction_2['amount'] >= each_transaction['amount'] * (1-amt_deviation)))
                    
                    date_obj_1 = each_transaction['date']
                    date_obj_2 = each_transaction_2['date']
                    if prev_matched_txn is not None:
                        prev_matched_date_obj = prev_matched_txn['date']
                        condition_date_diff = True if (date_obj_2 - prev_matched_date_obj).days >= 15 else False
                    else:
                        condition_date_diff = True if (date_obj_2 - date_obj_1).days >= 15 else False
                    
                    condition_days_diff = (abs(date_obj_2.day - date_obj_1.day) <= date_deviation) or (abs(date_obj_2.day - date_obj_1.day) >= (30-date_deviation))
                
                    if condition_amt_deviation and condition_date_diff and condition_days_diff:
                        match_list.append(each_transaction_2)
                        already_matched_list.add(each_transaction_2['hash'])
                        prev_matched_txn = each_transaction_2
        if len(match_list) > 1:
            final_matched_list.append(match_list)
    return final_matched_list


def calculate_fuzzy_scores(row1, row2):
    scores = {
        "fuzz_score_1": rfuzz.partial_token_sort_ratio(row1, row2),
        "fuzz_score_2": rfuzz.ratio(row1, row2),
        "fuzz_score_3": rfuzz.token_set_ratio(row1, row2),
    }
    return max(scores.values())


def get_top_salary_cluster(df, match_threshold=90):
    df = pl.DataFrame(df)
    credit_df = df.with_columns(
        pl.struct(df.columns).map_elements(lambda x: clean_transaction_note(x, ""), return_dtype=pl.Utf8).alias("clean_transaction_note")
    )
    credit_df = credit_df.sort(by="date", descending=False)
    transaction_list = credit_df.to_dicts()

    already_matched_list = set()
    final_matched_list = []

    for i_counter in range(len(transaction_list)):
        each_transaction = transaction_list[i_counter]
        match_list = []
        if each_transaction["hash"] not in already_matched_list:
            match_list.append(each_transaction)
            already_matched_list.add(each_transaction["hash"])
            for j_counter in range(i_counter + 1, len(transaction_list)):
                each_transaction_2 = transaction_list[j_counter]
                transaction_channel_1 = each_transaction["transaction_channel"]
                transaction_channel_2 = each_transaction_2["transaction_channel"]
                merchant_1 = str(each_transaction.get("unclean_merchant"))
                merchant_2 = str(each_transaction_2.get("unclean_merchant"))
                condition_0 = (transaction_channel_1 == transaction_channel_2) or (
                    transaction_channel_1 == "salary" or transaction_channel_2 == "salary"
                )

                condition_1 = each_transaction_2["hash"] not in already_matched_list

                condition_note_len = abs(len(each_transaction["clean_transaction_note"]) - len(each_transaction_2["clean_transaction_note"]) < 10)

                condition_merchan_len = len(merchant_1) > 10 and len(merchant_2) > 10

                if condition_0 and condition_1 and (condition_note_len or condition_merchan_len):
                    fuzz_partial_score = calculate_fuzzy_scores(
                        each_transaction["clean_transaction_note"], each_transaction_2["clean_transaction_note"]
                    )

                    if condition_0 and condition_1 and fuzz_partial_score >= match_threshold:
                        match_list.append(each_transaction_2)
                        already_matched_list.add(each_transaction_2["hash"])
        if len(match_list) > 1:
            final_matched_list.append(match_list)
    return final_matched_list
