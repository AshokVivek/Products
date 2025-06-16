import warnings
import pandas as pd
import re
from library.utils import NON_LENDER_MERCHANT_KEYWORDS
from library.helpers.constants import LOAN_TYPES
from rapidfuzz import fuzz
from library.enrichment_regexes import get_lender_list, get_lender_regex_list
from sentry_sdk import capture_message


warnings.simplefilter(action = "ignore", category = FutureWarning)
pd.options.mode.chained_assignment = None


lenders = []

removed_lender_list=['rhdfc','prolific','dhani','cashe','dmi']
#over_generic_lenders = ["fin", "dis"]

lender_small_words = ["finlease", 'fincorp', 'tradefinance', 'yesbankretai', 'draoperations', 'agrilnfullpayoff', 'goldlnfullpayoff', 'lenderfun']

# Other lender words which will be classified as lender transactions only when we catch the combination words as well.
other_lender_words = ['finance', 'agriln', 'stanchartbankclpu', 'personall', 'retailasse', 'plemi']

other_lender_combination_words = ["NACH", "ACH", "ECS", "MANDATE", "AUTOMATIC PAYMENT", "DISBURSAL", "DISBURSE", "CAR CREDIT", "HOUSING", "EDUCATION", "AUTO", "LOAN REPAYMENT", "LEASING", "RETAIL DISBURS"]

# if we catch the suffix words with lender words (lenders), then it won't be classified as lender transaction.
non_lender_suffix_words = ['liquor', 'wine', 'alcohol', 'stationery', 'fill', 'food', 'bike', 'motors', 'mobile', 'restauran','automobile', 'securi', 'invest', 'insurance', 'msgeneralinsurance']

# these are the regex through which we can catch a lender as well as if the transaction is a lender transaction or not
regex_list = ['(?i)^((?:LP|LB|LA))\s*[A-Z]{5}\s*[0-9]{5}\s*[a-z]{3}[0-9]{2}\s*[a-z]+.*',
                '(?i)^INS\s*DEBIT\s*A\\\c\s+((?:SPLN|PDL|CFB|CF|CSG|SA))\s*.*',
                '(?i).*TP\s*ACH\s+(ABL).*',
                '(?i)^DISB\s*:?[0-9]+\-(BL)\-.*',
                '(?i)^ECS\s+(BFL)\s+.*',
                '(?i).*[\/\- ]+(personal\s*(?:L|Lo|Loa))[\/\- ]+.*', 
                '(?i).*DIRECT\s*DEBIT.*((?:TCFSL|HDBFS|TCHFL|BAJAJ)).*',
                '.*[\/\-]+SAMPATI[\/\-]+.*',
                '.*[\/\-]+SAMPATI$',
                '.*[\/\-\_ ]+SIMPL[\/\-\_ ]+.*',
                '(?i).*[\-\/ ]+(PIRAMAL)[\-\/]+.*',
                '(?i).*[\-\/]+(PIRAMAL)[\-\/ ]+.*', 
                '(?i).*[\-\/]+(ADITYA\s*(?:BI|BIR|BIRL|BIRLA|BIRLAF))[\-\/]+.*',
                '(?i)^RTGS\/.*\/(BAJAJ\s*F)\/.*',
                '(?i)^ETPC\s*NEFT.*[\-\/ ]+((?:CHOLAMAN|PIRAMAL))$',
                '(?i).*[\/\- ]+(DISB)[\/\-]+.*',
                '(?i).*[\/\- ]+(ILEX)[\/\-]+.*',
                '(?i)^MMT[\/\- ]+IMPS[\/\- ]+[0-9]+[\/\- ]+(DISBURSAL).*',
                '(?i)^(PL\-ONLINE\s+DISBURSEMENT)\-[A-Z0-9]+',
                '(?i)^NEFT\s*CR\-[A-Z0-9]+\-(PL\s+DISBURSEMENT\s+SUSP )\-.*',
                '(?i)^[0-9]+(?:[A-Z]{1,4})?[0-9]+\s*(DISBURSEMENT\s*CREDIT)',
                '(?i)^MAND\s*DR\-\s+(BCF)[0-9]{4}\-.*',
                '(?i)^BE\s*\-\s*(ONLINE\s*DISBURSEMENT)\s*\-\s*[A-Z0-9]+',
                '(?i)^BY\s*TRANSFER\-(KCC\s*DISBURSED)\-',
                '(?i)^IMPS\/P2A\/[0-9]+\/(HDBFS)MUM\/[A-Z ]+\/.*',
                '(?i)^NEFT\:(HDBFS\s*MUMBAI\s*DISB).*',
                '(?i)^NEFT\/N[0-9]+\/HDFC\/(HDBFS\s*MUMBAI\s*DISB).*',
                '(?i)^[A-Z]{3}[0-9]+\:(DISB)\:.*',
                '(?i)^PABL\s*(DISBT)\s*[0-9]{2}\-[0-9]{2}',
                '(?i)^BY\s*TRANSFER\-\s*PABL\s*((?:DISBURSED|DISBRUSEM\sENT|DISBURSE\s*M\s*ENT|DISBURSE))\-',
                '(?i)^TRANSFER\s*[0-9]+\/INDUSIND\s*BANK\s*(CFD)$',
                '(?i)^(ECSBFL)[\: ]+[0-9]+\:?[0-9a-z]+$',
                '(?i)^(DISB)[0-9]{14}[\: ]*[0-9]{14}$',
                '(?i)^RTGS\/[A-Z0-9]+\/(HDBFS\s*MUMBAI\s*DISB)\s*(?:A\/C|AC).*',
                '(?i)^DEBIT\-ACHDR[A-Z0-9]+\sTP\sACH(CAPRIGL)\-',
                '(?i).*[^A-Z](RETAIL\sASSET\sBULK\s(?:LO|L)).*',
                '(?i)NEFT.*[\/|\*](RETAIL\sASSET\sBULK).*',
                '(?i).*[\/\- ]+(HDBFS\s*MUMBAI\s*DISB\s*A\/C).*',
                '(?i)^[0-9]{12,16}\s*(DISB)$',
                '(?i).*(HDFC\s*DISB\s*FUNDED).*',
                '(?i)^DISB[\:\/\- ]+[0-9]+[\-\/ ]+(PL)[\-\/ ]+.*',
                '(?i)^TRANSFER\s*FROM.*[\/\- ]+INB.*[\/\-]+(PL)[\/\- ]+.*',
                '(?i)^NEFT\-[A-Z0-9]+\-(FINOVA\s*(?:C|CA|CAP|CAPI|CAPIT))$',
                '(?i)^BY\sTRANSFER[\-\s]+INB\sIMPS[0-9]+\/[0-9 ]+\/[A-Z0-9 ]+\/(PL)\-A\-[0-9]+[\-\s]+.*',
                '(?i)^IMPS\-IN\/[0-9]+\/[0-9]+\/(ADITYA\s*(?:B|BI|BIR))$',
                '(?i)^RTGS\s*CR\-AUBL0002011\-180368\-[A-Z ]+[\-\s]+(AUBLR)[0-9 ]+.*',
                '(?i).*(HDBFS\s*(?:MU|MU|MUM|MUMB|MUMBA|MUMBAI))[^A-Z]+.*',
                '(?i).*[^A-Z]+(PROTIUM\s*(?:FI|FIN|FINA|FINAN|FINANC|FINANCE))(?:[^A-Z]+.*|\s*$)',
                '(?i)^(BL)\-ONLINE\s*DISBURSEMENT\-BL[0-9]+$',
                '(?i)^(?!.*SALE.*).*(POONAWAL).*',
                '(?i)^(?!.*CHEMMANUR\s*GOLD\s*PALA.*).*(CHEMMANUR).*',
                '(?i)^(?:.*[\/\- ]+|\s*)(KINARA\s*(?:CAPITAL|CAPITA|CAPIT|CAPI|CAP|CA|CAPITL|CPL|C))(?:[\/\- ]+.*|\s*)$']


# over generic wrong patterns
# fin_p = re.compile("(?i)(?:.*)fin[b-z].*")
# dis_p = re.compile("(?i)(?:.*)dis[ac-z].*")

# EMI pattern
emi_patt = re.compile('(?i).*[^A-Za-z]+(EMI)[^A-Za-z]+.*')
emi_patt2 = re.compile("^(?:.*[\\/\\-\\+\\_\\,\\@\\. ]+|\\s*)(EMI)(?:[\\/\\-\\+\\_\\,\\@\\. ]+.*|\\s*)$", flags=re.IGNORECASE)

loan_words = ['loan', 'losfunds']

map_dict = {'etyacol': 'Cashkumar',
            'akaracapital': 'StashFin',
            'zen lefin': 'Capital Float',
            'ivl finance': 'IndiaBulls',
            'bhanix': '5paisap2p',
            'ashishsec': 'earlysalary',
            'early': 'earlysalary',
            'flex': 'Flex Salary',
            'vivifiindia': 'Flex Salary',
            'whizdm': 'moneyview',
            'VISU LEASING': 'Incred',
            'camden': 'Zest Money',
            'sicreva': 'Kissht',
            'Goddard': 'Avail finance',
            'blue jay': 'Zip Loan',
            'muthfin': 'MUTHOOT',
            'RHDF': 'Religare Housing',
            'mahnimahi': 'mahindrafinance'
            }


def get_loan_transactions(df):
    not_allowed_list = ['cash_withdrawl', 'international_transaction_arbitrage', 'investment_cashin', 'cash_deposit', 'investment', 'self_transfer', 'bank_interest', 'demand_draft']
    allowed_df = df[~df['transaction_channel'].isin(not_allowed_list)]
    mask2 = (allowed_df['amount'] > 1000)
    allowed_df = allowed_df[mask2]

    if allowed_df.shape[0] > 0:
        allowed_df = allowed_df.apply(lambda x: check_loan(x), axis=1)
        loan_df = allowed_df[allowed_df['is_lender'] is True]
        loan_df.drop(columns=['is_lender'])
        return loan_df.to_dict('records')
    else:
        return []


def set_loan_row(row, lender, check_special_case=False):
    if row['transaction_channel'] == 'salary' and lender in ['disburs']:
        return row
    row['is_lender'] = True
    row['merchant'] = lender
    if row['transaction_channel'] == 'salary' and ((not check_special_case) or (check_special_case and lender in ['earlysalary', 'Flex Salary', 'FLEX', 'flex', 'early', 'loan'])):
        row['transaction_channel'] = 'net_banking_transfer'
    # set merchant category as loans
    if row['transaction_channel'] != 'salary' and row['merchant_category'] not in LOAN_TYPES:
        row['merchant_category'] = "loans"
    return row

# def set_unattended_obligation(row):
#     #print("****** >>>> in unattended obligation")
#     row['is_lender']='unidentified obligation'
#     return row



bank_related_lending_terms = {

    'ubi': ['^(bcf_).*'],
    'icici': ['^(ltcon).*','^(lpbng).*','^(vin/tcfsl).*']
}

def check_loan(row, country="IN"):
    global lenders
    
    lenders = get_lender_list(country)
    lender_regex_list_db_data = get_lender_regex_list(country)
    
    lender_regex_list = regex_list if lender_regex_list_db_data is None else lender_regex_list_db_data
    
    if not isinstance(lender_regex_list_db_data, list):
        try:
            capture_message("Could not get lender list from enrichment data. Using data in lender list file itself")
        except Exception as _:
            pass

    not_allowed_list = ['cash_withdrawl', 'international_transaction_arbitrage', 'investment_cashin', 'cash_deposit', 'investment', 'self_transfer', 'bank_interest', 'demand_draft']
    transaction_note = row['transaction_note']
    row['is_lender'] = False
    row['merchant'] = ''
    transaction_note = transaction_note.lower()
    # bank_name = row['bank_name']
    # bank_related_lending_term = bank_related_lending_terms.get(bank_name)
    # if bank_related_lending_term is not None:
    #     for each_term in bank_related_lending_term:
    #         # print(transaction_note, each_term)
    #         match = match_regex(transaction_note, each_term, 1)
    #         if match is not None:
    #             return set_loan_row(row,bank_name ,True)
    

    
    #transaction_note_1 = re.sub('[^A-Za-z ]+', '',  transaction_note)
    
    #For testing against the space used in regex
    transaction_note_1=re.sub('[\d]+','',transaction_note)
    transaction_note_1=re.sub('[^A-Za-z]+', '',  transaction_note_1)
    # print(transaction_note_1)
    #print("-----------------------------")
    if isinstance(transaction_note_1, str):
        transaction_channel = row['transaction_channel']
        if transaction_channel in not_allowed_list:
            return row
        
        for non_lender in NON_LENDER_MERCHANT_KEYWORDS:
            x = fuzz.partial_token_sort_ratio(non_lender, transaction_note_1)
            if x == 100 and len(transaction_note_1) > len(non_lender):
                return row 
        
        for each_lender in lenders:
            x = fuzz.partial_token_sort_ratio(each_lender, transaction_note_1)
            # print(each_lender,x)
            if x == 100 and len(transaction_note_1) >= len(each_lender):  
                #print("lender -> ",each_lender," score: ",x)
                for suffix_word in non_lender_suffix_words:
                    complete_lender = each_lender + suffix_word
                    y = fuzz.partial_token_sort_ratio(complete_lender, transaction_note_1)
                    if y == 100 and len(transaction_note_1) > len(complete_lender):
                        return row
                return set_loan_row(row, each_lender, True)


        for each_small_lender in lender_small_words:
            y = fuzz.partial_ratio(each_small_lender, transaction_note_1)
            if y == 100 and len(transaction_note_1) >= len(each_small_lender):
                #print("Small Lender ->",each_small_lender)
                return set_loan_row(row, each_small_lender,True)
            
        for word in other_lender_words:
            y = fuzz.partial_ratio(word, transaction_note_1)
            if y == 100 and len(transaction_note_1) > len(word):
                for combination_word in other_lender_combination_words:
                    y = fuzz.partial_ratio(combination_word.lower(), transaction_note_1)
                    if y == 100 and len(transaction_note_1) > len(combination_word):
                        return set_loan_row(row, word,True)

        for loan_word in loan_words:
            z = fuzz.partial_ratio(loan_word, transaction_note_1)
            if z == 100 and len(transaction_note_1) > len(loan_word):
                #print("Each lending word ->",each_lending_word)
                return set_loan_row(row, loan_word,True)

        #transaction_note = transaction_note.replace(".", "").replace("-", "")

        #EMI Logic on absolute transaction note
        transaction_note=" "+transaction_note+" "
        #print("Transaction Note: ",transaction_note,emi_patt.findall(transaction_note))
        if emi_patt.findall(transaction_note):
            return set_loan_row(row,'EMI',True)
        
        transaction_note = transaction_note.strip()
        
        for regex in lender_regex_list:
            regex = re.compile(regex)
            regex_match = re.match(regex, transaction_note)
            if regex_match is not None:
                return set_loan_row(row, regex_match.group(1), True)

        if row['merchant_category'] in LOAN_TYPES:
            row["is_lender"] = True

        # for each_over_generic_lender in over_generic_lenders:
        #     x = fuzz.partial_token_sort_ratio(each_over_generic_lender, transaction_note)
        #     if x > 90:
        #         if fin_p.search(transaction_note) or dis_p.search(transaction_note):
        #             break
        #         # print("NEW FOUND -> ", transaction_note)
        #         return set_loan_row(row, each_over_generic_lender, True)
    # (row['merchant_category'] != 'investments') and (row['merchant_category']!='trading/investments') and (row['merchant_category']!='insurance'):
    return row

def get_lenders_parameter_for_excel(df=None):
    # df = pd.read_csv('/Users/piyush/Downloads/sample_lender_date.csv')
    a = pd.DataFrame(get_loan_transactions(df))
    if a.shape[0] == 0:
        empty_df = pd.DataFrame(columns=['Lender Name', 'Number of Transactions', 'Average Amount', 'Total Amount',
                                         'Average Balance at Transaction'])
        return empty_df, empty_df
    credit_df = a[a['transaction_type'] == 'credit']
    debit_df = a[a['transaction_type'] == 'debit']

    credit_df = get_parameters(credit_df)
    debit_df = get_parameters(debit_df)
    return credit_df, debit_df


def get_parameters(df):
    grouped = df.groupby(['merchant'])
    return_df = pd.DataFrame(columns=['Lender Name', 'Number of Transactions', 'Average Amount', 'Total Amount',
                                      'Average Balance Before Transaction','First Date'])
    for each, each_df in grouped:
        obj = {}
        obj['Lender Name'] = each
        obj['Total Amount'] = int(each_df['amount'].sum())
        obj['Average Amount'] = int(each_df['amount'].mean())
        obj['Number of Transactions'] = each_df.shape[0]
        obj['First Date'] = each_df['date_stringtype'].min()
        if each_df.reset_index()['transaction_type'][0]=='credit':
            obj['Average Balance Before Transaction'] = int((each_df['balance']-each_df['amount']).mean())
        else:
            obj['Average Balance Before Transaction'] = int((each_df['balance']+each_df['amount']).mean())
        return_df = return_df.append(obj, ignore_index=1)
    return_df = return_df.sort_values('Number of Transactions',ascending=False)
    return_df = return_df[
        ['Lender Name', 'Number of Transactions', 'Average Amount', 'Total Amount', 'Average Balance Before Transaction','First Date']]
    return return_df
