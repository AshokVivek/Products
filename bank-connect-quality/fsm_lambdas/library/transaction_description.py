from library.lender_list import check_loan
from library.utils import match_compiled_regex, get_compiled_regex_list
from library.enrichment_regexes import get_description_regexes
from library.helpers.constants import BOUNCE_TRANSACTION_CHANNELS, TRANSACTION_CHANNELS
from fuzzywuzzy import fuzz
from rapidfuzz import fuzz as rfuzz
import re
from library.utils import remove_re_compile
from copy import deepcopy

ach_bounce_charge = []
chq_bounce_charge = []
penalty_charge = []
service_charge = []
card_issue = []
gst_regex = []
chq_bounce_insuff_funds = []
chq_technical_bounce = []
min_bal_charge = []
credit_card_bill = []
minimum_balance_charge = []
telco_bill = []
electric_bill = []
neft_return = []
deposit_machine_words = []
nach_setup_charge = []
stop_emi_charge = []
reimbursement = []
investment_income_regex = []
pension = []
cash_back = []
fixed_deposit = []
upi_settlement = []
card_settlement = []
dividend = []
subsidy = []
small_savings = []
cost = []
placement_of_deposit = []
disbursement_of_deposit = []
brokerage = []
share_purchase = []
share_sell = []
donation = []
crypto = []

def get_transaction_description(df, name, country="IN"):
    if country in [None, ""]:
        # default country is IN
        country = "IN"

    description_regexes = get_description_regexes(country)
    
    global ach_bounce_charge
    ach_bounce_charge = get_compiled_regex_list(description_regexes.get("ach_bounce_charge", []))
    
    global chq_bounce_charge
    chq_bounce_charge = get_compiled_regex_list(description_regexes.get("chq_bounce_charge", []))
    
    global penalty_charge
    penalty_charge = get_compiled_regex_list(description_regexes.get("penalty_charge", []))

    global service_charge
    service_charge = get_compiled_regex_list(description_regexes.get("service_charge", []))

    global card_issue
    card_issue = get_compiled_regex_list(description_regexes.get("card_issue", []))

    global gst_regex
    gst_regex = get_compiled_regex_list(description_regexes.get("gst_regex", []))

    global chq_bounce_insuff_funds
    chq_bounce_insuff_funds = get_compiled_regex_list(description_regexes.get("chq_bounce_insuff_funds", []))

    global chq_technical_bounce
    chq_technical_bounce = get_compiled_regex_list(description_regexes.get("chq_technical_bounce", []))

    global min_bal_charge
    min_bal_charge = get_compiled_regex_list(description_regexes.get("min_bal_charge", []))

    global credit_card_bill
    credit_card_bill = get_compiled_regex_list(description_regexes.get("credit_card_bill", []))

    global minimum_balance_charge
    minimum_balance_charge = get_compiled_regex_list(description_regexes.get("minimum_balance_charge", []))

    global telco_bill
    telco_bill = get_compiled_regex_list(description_regexes.get("telco_bill", []))

    global electric_bill
    electric_bill = get_compiled_regex_list(description_regexes.get("electric_bill", []))

    global neft_return
    neft_return = get_compiled_regex_list(description_regexes.get("neft_return", []))

    global deposit_machine_words
    deposit_machine_words = get_compiled_regex_list(description_regexes.get("deposit_machine_words", []))

    global nach_setup_charge
    nach_setup_charge = get_compiled_regex_list(description_regexes.get("nach_setup_charge", []))

    global stop_emi_charge
    stop_emi_charge = get_compiled_regex_list(description_regexes.get("stop_emi_charge", []))

    global reimbursement
    reimbursement = get_compiled_regex_list(description_regexes.get("reimbursement", []))

    global investment_income_regex
    investment_income_regex = get_compiled_regex_list(description_regexes.get("investment_income", []))

    global pension
    pension = get_compiled_regex_list(description_regexes.get("pension", []))

    global cash_back
    cash_back = get_compiled_regex_list(description_regexes.get("cash_back", []))

    global fixed_deposit
    fixed_deposit = get_compiled_regex_list(description_regexes.get("fixed_deposit", []))

    global upi_settlement
    upi_settlement = get_compiled_regex_list(description_regexes.get("upi_settlement", []))

    global card_settlement
    card_settlement = get_compiled_regex_list(description_regexes.get("card_settlement", []))

    global dividend
    dividend = get_compiled_regex_list(description_regexes.get("dividend", []))

    global subsidy
    subsidy = get_compiled_regex_list(description_regexes.get("subsidy", []))

    global small_savings
    small_savings = get_compiled_regex_list(description_regexes.get("small_savings", []))

    global cost
    cost = get_compiled_regex_list(description_regexes.get("cost", []))

    global placement_of_deposit
    placement_of_deposit = get_compiled_regex_list(description_regexes.get("placement_of_deposit", []))

    global disbursement_of_deposit
    disbursement_of_deposit = get_compiled_regex_list(description_regexes.get("disbursement_of_deposit", []))

    global clearing_withdrawal
    clearing_withdrawal = get_compiled_regex_list(description_regexes.get("clearing_withdrawal", []))

    global clearing_deposit
    clearing_deposit = get_compiled_regex_list(description_regexes.get("clearing_deposit", []))

    global brokerage
    brokerage = get_compiled_regex_list(description_regexes.get("brokerage", []))

    global share_purchase
    share_purchase = get_compiled_regex_list(description_regexes.get("share_purchase", []))

    global share_sell
    share_sell = get_compiled_regex_list(description_regexes.get("share_sell", []))

    global rent_received
    rent_received = get_compiled_regex_list(description_regexes.get("rent_received", []))

    global donation
    donation = get_compiled_regex_list(description_regexes.get("donation", []))

    global crypto
    crypto = get_compiled_regex_list(description_regexes.get("crypto", []))

    df = df.apply(lambda x: transaction_description(x, name, country), axis=1)
    return df

def _get_transfer_description(row: dict) -> dict:
    _unclean_merchant = row['unclean_merchant']
    if row['transaction_type'] == 'debit' and _unclean_merchant:
        row['description'], row['description_regex'] = f"Transfer to {_unclean_merchant}", ""

    elif row['transaction_type'] == 'credit' and _unclean_merchant:
        row['description'], row['description_regex'] = f"Transfer from {_unclean_merchant}", ""
    
    return row

def transaction_description(row, name, country):
    transaction_channel = row['transaction_channel']
    transaction_note = row['transaction_note']
    transaction_type = row["transaction_type"]
    merchant_category = row["merchant_category"]
    row['description'] = ''
    row["description_regex"] = "" # default description regex is a blank string
    row['is_lender'] = False
    row['merchant'] = ''
    # print("\n\n", row)
    
    # transaction channel related transaction description
    if transaction_channel in [TRANSACTION_CHANNELS.SALARY, TRANSACTION_CHANNELS.REFUND, TRANSACTION_CHANNELS.REVERSAL]:
        return _get_transfer_description(row)
    
    if transaction_channel in [TRANSACTION_CHANNELS.INWARD_CHEQUE_BOUNCE, TRANSACTION_CHANNELS.OUTWARD_CHEQUE_BOUNCE] + BOUNCE_TRANSACTION_CHANNELS:
        row['description'], row['description_regex'] = get_chq_bounce_description(transaction_note)
    
    if transaction_channel == TRANSACTION_CHANNELS.CASH_WITHDRAWL:
        return row
    
    if transaction_channel == TRANSACTION_CHANNELS.CASH_DEPOSIT:
        row['description'], row['description_regex'] = get_cash_deposit_description(transaction_note)
        return row
    
    if merchant_category in ['investments', 'trading/investments', 'insurance', 'mutual_funds']:
        # tag description as investment only when we didn't cought any other description
        row['description'], row['description_regex'] = row['merchant_category'], ''
        return row
    

    if transaction_channel == 'bank_charge':
        row['description'], row['description_regex'] = get_bank_charge_description(transaction_note)
        if row['description'] in ["ach_bounce_charge", "chq_bounce"]:
            temp_row = deepcopy(row)
            temp_row = check_loan(temp_row, country)
            if temp_row["is_lender"]:
                row['merchant_category'] = "loan_bounce"
    else:
        row = check_loan(row, country)
        if row['is_lender'] and transaction_channel != TRANSACTION_CHANNELS.SALARY:
            row['description'], row['description_regex'] = 'lender_transaction', ''

        elif merchant_category in ["investments", "trading/investments"] and transaction_type == "credit":
            row["description"], row["description_regex"] = get_investment_income(transaction_note)
        elif (row['is_lender'] == False) and (row['transaction_channel'] == 'auto_debit_payment') and (row['merchant_category'] not in ['investments','trading/investments','insurance', 'mutual_funds']):
            row['description'], row['description_regex'] = 'unidentified_obligation', ''   
        elif transaction_channel in ['net_banking_transfer', 'upi', 'chq','bi_fast'] or (country == 'ID' and transaction_channel in ['auto_credit']) and row["description"] not in ["pension"]:
            row['description'], row['description_regex'] = get_self_transfer_description(transaction_note, row['unclean_merchant'], name, country)
        else:
            if transaction_type=="credit" and row["description"]=="":
                row['description'], row['description_regex'] = get_pension(transaction_note)
            if row["description"] in (None, ""):
                row['description'], row['description_regex'] = get_gst_description(transaction_note) 
    
    if row['description'] == '' and transaction_channel == 'net_banking_transfer' and country in ['ID']:
        # Reimbursent is only for Indonesian Banks right now, whenever the transaction is net banking, prioritze reimbursent
        row['description'], row['description_regex'] = get_reimbursement(transaction_note)
    
    if row['description'] == '':
        row['description'], row['description_regex'] = get_description_from_list(transaction_note, cash_back, 'cash_back')
    if row['description'] == '':
        row['description'], row['description_regex'] = get_description_from_list(transaction_note, fixed_deposit, 'fixed_deposit')
    if row['description'] == '':
        row['description'], row['description_regex'] = get_description_from_list(transaction_note, upi_settlement, 'upi_settlement')
    if row['description'] == '':
        row['description'], row['description_regex'] = get_description_from_list(transaction_note, card_settlement, 'card_settlement')
    if row['description'] == '':
        row['description'], row['description_regex'] = get_description_from_list(transaction_note, dividend, 'dividend')
    if row['description'] == '':
        row['description'], row['description_regex'] = get_description_from_list(transaction_note, subsidy, 'subsidy')
    if row['description'] == '':
        row['description'], row['description_regex'] = get_description_from_list(transaction_note, small_savings, 'small_savings')

    # across all transaction types
    if row['description'] == "":
        row['description'], row['description_regex'] = get_description_from_list(transaction_note, brokerage, 'brokerage')
    if row['description'] == "":
        row['description'], row['description_regex'] = get_description_from_list(transaction_note, crypto, 'crypto')

    # for transaction_type debit
    if row['description'] in ["", "unidentified_obligation"] and transaction_type=="debit":
        row['description'], row['description_regex'] = get_bill_description(transaction_note)
    if row['description'] == "" and transaction_type == "debit":
        row['description'], row['description_regex'] = get_description_from_list(transaction_note, share_purchase, 'share_purchase')
    if row["description"] == "" and transaction_type == "debit":
        row['description'], row['description_regex'] = get_description_from_list(transaction_note, donation, 'donation')


    # for transaction_type credit
    if row['description'] == '' and transaction_type == "credit":
        row['description'], row['description_regex'] = get_description_from_list(transaction_note, rent_received, 'rent_received')
    if row['description'] == "" and transaction_type == "credit":
        row['description'], row['description_regex'] = get_description_from_list(transaction_note, share_sell, 'share_sell')
    
    
    
    if country in ['ID']:
        if row['description'] == '':
            row['description'], row['description_regex'] = get_description_from_list(transaction_note, cost, 'cost')
        if row['description'] == '':
            row['description'], row['description_regex'] = get_description_from_list(transaction_note, placement_of_deposit, 'placement_of_deposit')
        if row['description'] == '':
            row['description'], row['description_regex'] = get_description_from_list(transaction_note, disbursement_of_deposit, 'disbursement_of_deposit')
        if row['description'] == '':
            row['description'], row['description_regex'] = get_description_from_list(transaction_note, clearing_withdrawal, 'clearing_withdrawal')
        if row['description'] == '':
            row['description'], row['description_regex'] = get_description_from_list(transaction_note, clearing_deposit, 'clearing_deposit')
    
    if row['description'] == '' and transaction_channel not in [TRANSACTION_CHANNELS.INWARD_CHEQUE_BOUNCE, TRANSACTION_CHANNELS.OUTWARD_CHEQUE_BOUNCE] + BOUNCE_TRANSACTION_CHANNELS:
        row = _get_transfer_description(row)
        
    return row

def get_gst_description(transaction_note):
    if isinstance(transaction_note, str):
        transaction_note = transaction_note.upper()
        for gst in gst_regex:
            match = match_compiled_regex(transaction_note, gst, 1)
            if match is not None:
                return 'gst', remove_re_compile(gst)

    return '', None


def get_investment_income(transaction_note):
    if isinstance(transaction_note, str):
        transaction_note = transaction_note.upper()
        for reg in investment_income_regex:
            match = match_compiled_regex(transaction_note, reg, 1)
            if match is not None:
                return 'investment_income', remove_re_compile(reg)

    return '', None

def get_pension(transaction_note):
    if isinstance(transaction_note, str):
        transaction_note = transaction_note.upper()
        for reg in pension:
            match = match_compiled_regex(transaction_note, reg, 1)
            if match is not None:
                return 'pension', remove_re_compile(reg)

    return '', None

def get_bank_charge_description(transaction_note):
    if isinstance(transaction_note, str):
        transaction_note = transaction_note.upper()

        for word in minimum_balance_charge:
            match = match_compiled_regex(transaction_note, word, 1)
            if match is not None:
                return 'min_bal_charge', remove_re_compile(word)

        for word in chq_bounce_charge:
            match = match_compiled_regex(transaction_note, word, 1)
            if match is not None:
                return 'chq_bounce_charge', remove_re_compile(word)

        for word in ach_bounce_charge:
            match = match_compiled_regex(transaction_note, word, 1)
            if match is not None:
                return 'ach_bounce_charge', remove_re_compile(word)

        for word in penalty_charge:
            match = match_compiled_regex(transaction_note, word, 1)
            if match is not None:
                return 'penalty_charge', remove_re_compile(word)

        for word in service_charge:
            match = match_compiled_regex(transaction_note, word, 1)
            if match is not None:
                return 'service_charge', remove_re_compile(word)

        for word in card_issue:
            match = match_compiled_regex(transaction_note, word, 1)
            if match is not None:
                return 'card_issue_charge', remove_re_compile(word)
        
        for word in min_bal_charge:
            match = match_compiled_regex(transaction_note, word, 1)
            if match is not None:
                return 'min_bal_charge', remove_re_compile(word)

        for word in gst_regex:
            match = match_compiled_regex(transaction_note, word, 1)
            if match is not None:
                return 'gst', remove_re_compile(word)
            
        for word in nach_setup_charge:
            match = match_compiled_regex(transaction_note, word, 1)
            if match is not None:
                return 'nach_setup_charge', remove_re_compile(word)

        for word in stop_emi_charge:
            match = match_compiled_regex(transaction_note, word, 1)
            if match is not None:
                return 'stop_emi_charge', remove_re_compile(word) 

    return '', ''


def get_bill_description(transaction_note):
    if isinstance(transaction_note, str):
        transaction_note = transaction_note.upper()
        for cc_word in credit_card_bill:
            match = match_compiled_regex(transaction_note, cc_word, 1)
            if match is not None:
                return 'credit_card_bill', remove_re_compile(cc_word)

        for telco_word in telco_bill:
            match = match_compiled_regex(transaction_note, telco_word, 1)
            if match is not None:
                return 'telco_bill', remove_re_compile(telco_word)
        
        for electric_word in electric_bill:
            match = match_compiled_regex(transaction_note, electric_word, 1)
            if match is not None:
                return 'electric_bill', remove_re_compile(electric_word)

    return '', ''


def get_chq_bounce_description(transaction_note):
    if isinstance(transaction_note, str):
        transaction_note = transaction_note.upper()
        for chq_word in chq_bounce_insuff_funds:
            match = match_compiled_regex(transaction_note, chq_word, 1)
            if match is not None:
                return 'chq_bounce_insuff_funds', remove_re_compile(chq_word)
            
        for chq_word in chq_technical_bounce:
            match = match_compiled_regex(transaction_note, chq_word, 1)
            if match is not None:
                return 'chq_technical_bounce', remove_re_compile(chq_word)

    return '', ''


def get_cash_deposit_description(transaction_note):
    if isinstance(transaction_note, str):
        transaction_note = transaction_note.upper()
        for deposit_machine_word in deposit_machine_words:
            match = match_compiled_regex(transaction_note, deposit_machine_word, 1)
            if match is not None:
                return 'deposit_by_machine', remove_re_compile(deposit_machine_word)

    return '', ''


# def get_self_transfer_description(transaction_note, name):
#     if isinstance(transaction_note, str):
#         transaction_note = transaction_note.upper()
#         if isinstance(name, str):
#             name = name.upper()
#             x = fuzz.partial_ratio(name, transaction_note)
#             if x > 90:
#                 return 'self_transfer'
#     return ''

def get_reimbursement(transaction_note):
    if isinstance(transaction_note, str):
        transaction_note = transaction_note.upper()
        for reimbursement_word in reimbursement:
            match = match_compiled_regex(transaction_note, reimbursement_word, 1)
            if match is not None:
                return 'reimbursement', remove_re_compile(reimbursement_word)
    return '', ''

def get_description_from_list(transaction_note, regex_list, tag):
    if isinstance(transaction_note, str):
        transaction_note = transaction_note.upper()
        for desc_word in regex_list:
            match = match_compiled_regex(transaction_note, desc_word, 1)
            if match is not None:
                return tag, remove_re_compile(desc_word)
    return '', ''


def get_self_transfer_description(transaction_note, unclean_merchant, name, country = 'IN'):
    if isinstance(name, str):
        clean_name = cleanNameTokens(name, country)
        clean_name_without_spaces = clean_name.replace(" ", "")
        
        if isinstance(unclean_merchant, str) and len(unclean_merchant)>3:
            if country in ['ID']:
                unclean_merchant = remove_unwanted_pattern_in_unclean_merchant(unclean_merchant, country)
            unclean_merchant = unclean_merchant.lower()
            if clean_name in unclean_merchant:
                return 'self_transfer', ''
            
            wratio = rfuzz.WRatio(clean_name, unclean_merchant)
            if wratio>87:
                return 'self_transfer', ''
            
            unclean_merchant_without_spaces = unclean_merchant.replace(" ", "")
            wratio = rfuzz.WRatio(clean_name_without_spaces, unclean_merchant_without_spaces)
            if wratio>87:
                return 'self_transfer', ''
            
            unclean_merchant = clean_trxn_note(unclean_merchant)
            wratio = rfuzz.WRatio(clean_name, unclean_merchant)
            if wratio>87:
                return 'self_transfer', ''
            
        elif country not in ['ID'] and isinstance(transaction_note, str):
            transaction_note = transaction_note.lower()
            
            if clean_name in transaction_note:
                return 'self_transfer', ''
            
            wratio = rfuzz.WRatio(clean_name, transaction_note)
            if wratio>87:
                return 'self_transfer', ''
    
    return '', ''


def clean_trxn_note(transaction_note):
    transaction_note = re.sub(r'[\/\,\-\.]', ' ', transaction_note)
    return transaction_note


def cleanNameTokens(name, country = 'IN'):
    nameCheck = name.lower() + ' '
    listRelations = ['s/o','d/o','w/o','c/o','r/o',
                     'son of', 'daughter of', 'wife of', 'care of', 'resident of',
                     's / o', 'd / o', 'w / o', 'c / o', 'r / o',
                     's /o', 'd /o', 'w /o', 'c /o', 'r /o',
                     's/ o', 'd/ o', 'w/ o', 'c/ o', 'r/ o',
                     ' so ',' do ', ' wo ', ' co ', ' ro ',
                     ' s o ',' d o ', ' w o ', ' c o ', ' r o ']
                     
    mohdVariations = ['md','mohd','mo', 'mohammed', 'Muhammed',
                      'mohammad', 'mohmd', 'muhammad', 'muhamad', 'mohamed',
                      'mohmed','mohamad']

    for x in listRelations:
        if x in nameCheck:
            indexCheck = nameCheck.find(x)
            nameCheck = nameCheck[0:indexCheck]
    
    listReplace = ['m/s','Messers', 'm / s', 'm/ s', 'm /s']
    
    for x in listReplace:
        nameCheck = nameCheck.replace(x, '')
    

    tokensName = re.sub(r"\d+", " ", re.sub(r"\W+", " ", nameCheck)).split(" ")
    
    listPrefix = ['mr', 'mrs', 'miss', 'master', 'dr', 'ms' , 'st' , 'sr' , 'jr', 'eng']
    
    tokensFinal = [x for x in tokensName if x not in listPrefix and x != '' and x != len(x) * 'x']
    listSuffix = ['bhai','ben','behen','bai','kumar','lal', 'kumari', 'kr']
                
    words_to_remove=['pvt','ltd','private','limited','corporation','technologies','technology','india', 'software','solutions',
                     'solution','bank','banks','enterprises','enterprise',
                     'com', 'and', 'the',
                     'international', 'loan', 'loans', 'group', 
                     'support', 'operations', 'operation', 'systems', 'corp', 
                     'properties', 'industry', 'industries', 
                     'automotives', 'exports', 'tech', 'insurance', 'centre', 
                     'credit', 'textile', 'textiles', 'net',
                     'restaurant', 'finance','services', 'service',
                     'i','me','my','am', 'is', 'are', 'was', 'be', 'has','had','a', 'in',
                     'an','the', 'and','if', 'or', 'of', 'at', 'by', 'for', 'to', 'on', 'it',
                     'industri', 'pt', 'pte', 'cv', 'corpora', '( idr )', '(idr)', 'etransaksi', 'e transaksi'
                    ]
            
    tokensFinal = [x for x in tokensFinal if x != '' and x not in mohdVariations]
    tokensUnique = [x for x in tokensFinal if x != '' and x not in words_to_remove]
    if country in ['ID']:
        tokensUnique = tokensUnique[:2]
    
    if len(tokensUnique)>0:
        clean_name = ' '.join(tokensUnique)
        if len(clean_name)>3:
            return clean_name
        
        clean_name = ' '.join(tokensFinal)
        return clean_name
    else:
        return 'None'


def remove_unwanted_pattern_in_unclean_merchant(unclean_merchant, country):
    if country == 'ID':
        patterns_to_remove = ['^pt ', ' pt$', '^pte ', ' pte$', 
                                '^cv ', ' cv$', '^industri ', ' industri$', '^industri ', ' industri$', 
                                '^corpora ', ' corpora$',
                                '^etransaksi ', ' etransaksi$']
        pattern = re.compile("|".join(["("+s+")" for s in patterns_to_remove]))
        unclean_merchant = re.sub(pattern, '', unclean_merchant.strip().lower())
    return unclean_merchant
