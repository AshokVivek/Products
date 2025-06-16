import re
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
# from statistics import stdev,mean
import warnings
import pandas as pd
from copy import deepcopy
from rapidfuzz import fuzz as rfuzz

from library.recurring_transaction import (
    get_credit_recurring_transactions,
    multiple_split,
    get_recurring_using_dict_with_fuzz_threshold,
    get_top_salary_cluster,
    get_all_transaction_fuzz_score,
    get_recurring_subgroups,
)
from library.helpers.constants import MIN_SALARY_AMOUNT_FOR_KEYWORD_CLASSIFICATION


warnings.simplefilter(action = "ignore", category = FutureWarning)
pd.options.mode.chained_assignment = None


AMOUNT_BIN_SIZE = 4000
DATE_THRESHOLD = 20
STANDARD_DEVITATION_LIMIT_2CREDITS = 0.3
MINIMUM_PERMISSIBLE_SALARY_INR = 3000

def does_unclean_merchant_exists_in_employer_names(unclean_merchant, employer_names=[]):
    if not unclean_merchant:
        return False
    for employer_name in employer_names:
        if unclean_merchant.lower() in employer_name.lower():
            return True
    return False

def get_salary_from_employer_names(df, employer_names):
    df_copy = deepcopy(df)
    employer_names_clean_word_dict = {} 
    if df_copy is not None:
        split = [" ", "/", ":", "-", ",", ";", ".", "(", ")"]

        final_words=[]

        words_to_remove=['pvt','ltd','private','limited','corporation','technologies','technology','india', 'software','solutions','solution','bank','banks','enterprises','enterprise',
                        # New changes
                        'com', 'and', 'the', 'in',
                        'international', 'loan', 'loans', 'group', 
                        'support', 'operations', 'operation', 'systems', 'corp', 
                        'properties', 'industry', 'industries', 
                        'automotives', 'exports', 'tech', 'insurance', 'centre', 
                        'capital', 'credit', 'textile', 'textiles', 'net',
                        'restaurant', 'finance','services', 'service',
                        'i','me','my','am', 'is', 'are', 'was', 'be', 'has','had','a',
                        'an','the', 'and','if', 'or', 'of', 'at', 'by', 'for', 'to', 'on', 'it']
        for employer in employer_names:
            employer=employer.lower()

            pattern = re.compile(r'\b(?:{})\b'.format('|'.join(words_to_remove)))
            tokens = multiple_split(employer, split)
            tokens = [token for token in tokens if not pattern.search(token)]
            cleaned_employer_name = ' '.join(tokens)

            if len(cleaned_employer_name)>2 and cleaned_employer_name not in final_words and cleaned_employer_name not in words_to_remove:
                final_words.append(cleaned_employer_name)
                employer_names_clean_word_dict[cleaned_employer_name] = employer

        for ind in (df_copy.index):
            transaction_note=str(df_copy['transaction_note'][ind]).lower()
            unclean_merchant=str(df_copy['unclean_merchant'][ind]).lower()
            flag=False
            for word in final_words:
                partial_token_sort_ratio = rfuzz.partial_token_sort_ratio(transaction_note,word)
                wratio = rfuzz.WRatio(transaction_note,word)
                if word in transaction_note or partial_token_sort_ratio>90 or wratio>80:
                    flag=True
                    df_copy.loc[ind, 'employer_name'] = employer_names_clean_word_dict.get(word, '')
            
            if not flag:
                flag = does_unclean_merchant_exists_in_employer_names(unclean_merchant, employer_names)

            if flag is False:
                df_copy.drop(ind,inplace=True)
        return df_copy
    
    else:
        return pd.DataFrame()


def keyword_check(df):
    if df.shape[0] == 0:
        return pd.DataFrame(), pd.DataFrame()  # Return empty DataFrames if input is empty

    channel_allowed_list = ["net_banking_transfer", "salary", "Other", "other"]
    ignore_salary_patterns = ".*early\s*salary.*|.*esloan.*|.*flex\s*salary.*|.*cash\s*loan.*|.*loan\s*disb.*"
    p2_transactions = ".*p2p.*|.*p2m.*|.*p2g.*|.*p2b.*|.*p2a.*|.*p2l.*|.*p2o.*|.*p2e.*"

    # Conditions for passing checks
    pass_conditions = (
        (df["transaction_type"] == "credit")
        & (df["transaction_channel"].isin(channel_allowed_list))
        & (df["merchant_category"] != "loans")
        & (df["description"] != "lender_transaction")
        & (~df["transaction_note"].str.contains(ignore_salary_patterns, flags=re.IGNORECASE))
        & (~df["transaction_note"].str.contains(p2_transactions, flags=re.IGNORECASE))
    )

    # DataFrames for passing and failing the conditions
    check_pass = df[pass_conditions]
    check_fail = df[~pass_conditions]

    return check_pass, check_fail


def get_debit_transaction(transactions):
    df = pd.DataFrame(transactions)
    if df.shape[0] == 0:
        return None

    p2_transactions = ".*p2p.*|.*p2m.*|.*p2g.*|.*p2b.*|.*p2a.*|.*p2l.*|.*p2o.*|.*p2e.*"
    not_allowed_patterns = [
        ".*[^a-z](nach|ecs|ach|achdr)[^a-z]+.*",
        "^(ach|nach|ecs|achdr)[^a-z].*",
        "^ACHDR(\\/)?.*\\/[0-9]{4,}\\/[0-9]{4,}.*",
        "^(Loan)?\\s*Reco.*For\\s*[0-9]{3,}\\s*",
        ".*premium.*",
    ]
    not_allowed_channel = ["auto_debit_payment"]

    finall_pattern = ""
    for p in not_allowed_patterns:
        finall_pattern += p + "|"
    finall_pattern = finall_pattern[:-1]

    debit_df = df[
        (df["transaction_type"] == "debit")
        & (df["merchant_category"] != "loans")
        & (df["description"] != "lender_transaction")
        & (~df["transaction_note"].str.contains(p2_transactions, flags=re.IGNORECASE))
        & (~df["transaction_note"].str.contains(finall_pattern, flags=re.IGNORECASE))
        & (~(df["transaction_channel"].isin(not_allowed_channel)))
    ]
    if debit_df.shape[0] > 0:
        return debit_df

    return None


def get_topk_credit_transactions_each_month(df, k=2):
    if df is None:
        return pd.DataFrame()
    # Filter out rows with 'UPI' or 'IMPS' in the transaction_note column
    df = df[~df["transaction_note"].str.contains("UPI", flags=re.IGNORECASE, na=False)]
    df = df[~df["transaction_note"].str.contains("IMPS", flags=re.IGNORECASE, na=False)]
    excluded_words = ["Paytm", "Phonepe", "Bhim", "Gpay"]
    df = df[~df["transaction_note"].str.contains("|".join(excluded_words), case=False, na=False)]

    # Convert date strings to datetime
    df["date"] = pd.to_datetime(df["date"])
    today = datetime.today()
    six_months_ago_start = (today.replace(day=1) - pd.DateOffset(months=6)).replace(day=1)
    df = df[df["date"] >= six_months_ago_start]

    # Create 'year_month' period column
    df["year_month"] = df["date"].dt.to_period("M")

    # Sort the DataFrame by year_month and transaction amount descending
    df_sorted = df.sort_values(["year_month", "amount"], ascending=[True, False])

    # Group by 'year_month' and select the top k rows from each group
    top_k_per_month = df_sorted.groupby("year_month").head(k)

    top_k_per_month.drop(["year_month"], axis=1, inplace=True)
    return top_k_per_month


def transaction_is_top_transaction_of_month(top_k_transaction, df, median_amount, band):
    # Define the 20% range
    lower_bound = median_amount * (1 - band)
    upper_bound = median_amount * (1 + band)
    df["date"] = pd.to_datetime(df["date"])
    top_k_transaction["date"] = pd.to_datetime(top_k_transaction["date"])

    matching_rows = pd.merge(df, top_k_transaction, on=["date", "amount"], suffixes=("", "_drop"))[df.columns]
    fp_handling = pd.DataFrame()
    if matching_rows.shape[0] > 0:
        fp_handling = df[(df["amount"] >= lower_bound) & (df["amount"] <= upper_bound)]
    return fp_handling

def same_employer_checks(df):
    if df.shape[0] == 0:
        return pd.DataFrame()

    channel_allowed_list = ["net_banking_transfer", "salary", "Other", "other"]
    ignore_salary_patterns = ".*early\s*salary.*|.*esloan.*|.*flex\s*salary.*|.*cash\s*loan.*|.*loan\s*disb.*|.*expense.*|.*loan.*|.*investment.*|.*invoice.*|.*advance.*|mbk.*|.*inft.*|upi.*"
    p2_transactions = ".*p2p.*|.*p2m.*|.*p2g.*|.*p2b.*|.*p2a.*|.*p2l.*|.*p2o.*|.*p2e.*"

    # Conditions for passing checks
    pass_conditions = (
        (df["transaction_type"] == "credit")
        & (df["transaction_channel"].isin(channel_allowed_list))
        & (df["merchant_category"] != "loans")
        & (df["description"] != "lender_transaction")
        & (~df["transaction_note"].str.contains(ignore_salary_patterns, flags=re.IGNORECASE))
        & (~df["transaction_note"].str.contains(p2_transactions, flags=re.IGNORECASE))
    )

    # DataFrames for passing and failing the conditions
    check_pass = df[pass_conditions]
    return check_pass

def get_salary_transactions_v1(transactions,employer_names=[],recurring_salary_flag=False,salary_mode = 'HARD', probable_salary_transaction_groups = [], salary_configuration = {}):
    standard_deviation_limit, min_salary_cnt_allowed, min_salary_amount, min_salary_amount_keyword_method = extract_salary_variables(salary_mode, salary_config = salary_configuration)

    month_list = get_month_list_from_transactions(transactions)
    last_6month_threshold = (datetime.strptime(month_list[-1], "%b-%Y") - relativedelta(months=5))
    #Remove None values from employer names
    employer_names = [i for i in employer_names if i is not None]
    all_transactions = pd.DataFrame(transactions)
    if 'chq_num' in all_transactions.columns:
        all_transactions['chq_num'] = all_transactions['chq_num'].fillna('')
    num_diff_month = get_num_diff_month(all_transactions)
    df = get_credit_df(transactions, min_salary_amount_keyword_method)
    if df is not None:
        df['employer_name'] = ''
    complete_credit_df = get_complete_credit_df(transactions, min_salary_amount)
    if complete_credit_df is not None:
        complete_credit_df['employer_name'] = ''
    # confidence_percentage=0
    if df is not None or complete_credit_df is not None:
        df_salary = pd.DataFrame()
        if df is not None:
            df=df.fillna('')
            df_salary = df[df['transaction_channel'] == 'salary']
            df = df[(df['amount']>min_salary_amount) &
                    (~df['transaction_note'].str.contains('UPI', flags=re.IGNORECASE))]
            if 'salary_calculation_method' in df_salary.columns:
                '''
                    df_salary is the dataframe including transctions that have been marked as salary with keyword. Therefore, excluding recurring and employer_name.
                    employer_name and recurring salary transactions are anyways again calculated below in employer_salary_df & salary_df.
                '''
                df_salary = df_salary[
                    (df_salary['salary_calculation_method'] != 'recurring') &
                    (df_salary['salary_calculation_method'] != 'employer_name')
                ]
            # Assigning calculation_method equal to keyword for those transactions that have been classified to salary by keyword in fsmlib.
            df_salary = df_salary.assign(calculation_method='keyword')

        employer_salary_df = pd.DataFrame()
        if len(employer_names) > 0 and complete_credit_df is not None:
            employer_salary_df = get_salary_from_employer_names(complete_credit_df,employer_names)
            employer_salary_df = employer_salary_df.assign(calculation_method='employer_name')

        employer_salary_keyword_df =pd.concat([employer_salary_df,df_salary])
        if len(employer_salary_keyword_df) > 0:
            employer_salary_keyword_df = employer_salary_keyword_df.drop_duplicates(subset=['hash'],keep='first')
        employer_salary_keyword_df.reset_index(drop=True, inplace=True)

        if len(employer_salary_keyword_df) > 0:
            recurring_credits_list = probable_salary_transaction_groups
            # Add all transactions from Same Employer
            for recur_credit in recurring_credits_list:
                employer_hashes = set(employer_salary_keyword_df['hash'])
                recur_credit_hashes = {rc['hash'] for rc in recur_credit}
                hash_match = bool(employer_hashes & recur_credit_hashes)
                if hash_match:
                    matched = pd.DataFrame(recur_credit)
                    matched = matched.assign(calculation_method='recurring')
                    matched = same_employer_checks(matched)
                    employer_salary_keyword_df = pd.concat([employer_salary_keyword_df,matched])
            employer_salary_keyword_df = get_credit_df(employer_salary_keyword_df,min_salary_amount_keyword_method)
            
            if employer_salary_keyword_df is None:
                return dict()
            
            employer_salary_keyword_df = employer_salary_keyword_df.drop_duplicates(subset=['hash'],keep='first')
            channel_allowed_list = ["net_banking_transfer", "salary", "Other", "other"]
            employer_salary_keyword_df = employer_salary_keyword_df[employer_salary_keyword_df["transaction_channel"].isin(channel_allowed_list)]
            
            return_data = {}
            num_diff_month = get_num_diff_month(employer_salary_keyword_df)
            group_keyword_salaries = get_recurring_subgroups(employer_salary_keyword_df,amt_deviation=0.2,date_deviation=6)
            median_sal_amt = 0
            for keyword_grp in group_keyword_salaries:
                if len(keyword_grp) >= (num_diff_month-2):
                    median_sal_amt = max(median_sal_amt,max([trnx['amount'] for trnx in keyword_grp]))
            if median_sal_amt < min_salary_amount:
                median_sal_amt=employer_salary_keyword_df['amount'].max()
            employer_salary_keyword_df = get_salary_processed_data(employer_salary_keyword_df, month_list, standard_deviation_limit, ignore_deviation=True,median_salary_amount=median_sal_amt)
            
            if employer_salary_keyword_df.empty:
                return dict()
            salary_transactions =employer_salary_keyword_df.to_dict('records')
            # salary_transactions = sorted(salary_transactions, key=lambda x: x['date'])
            cnt_salary_months = employer_salary_keyword_df['salary_month'].nunique()

            return_data['salary_transactions'] = salary_transactions
            return_data['avg_monthly_salary_amount'] = (employer_salary_keyword_df.amount.sum() / cnt_salary_months) if cnt_salary_months > 0  else 0.0
            return_data['num_salary_months'] = cnt_salary_months
            return_data['latest_salary_date'] = employer_salary_keyword_df.date.max()
            return_data['latest_salary_amount'] = employer_salary_keyword_df[employer_salary_keyword_df['date'] == return_data['latest_salary_date']].amount.max()
            return_data['confidence_percentage'] = 100
            return return_data
        elif recurring_salary_flag and df is not None:
            recurring_credits_list = probable_salary_transaction_groups
            possible_salaries = []
            possible_salaries_with_2credits = []
            recurring_subgroups_list = []
            for each_recurring_credit in recurring_credits_list:
                each_recurring_credit_df_all_amount = pd.DataFrame(each_recurring_credit)
                each_recurring_credit_df = each_recurring_credit_df_all_amount[each_recurring_credit_df_all_amount['amount'] > min_salary_amount]
                # TODO remove random columns from each_recurring_credit_df
                num_diff_months_txns = get_num_diff_month(
                    each_recurring_credit_df)
                if (each_recurring_credit_df.shape[0] > 1) & ((num_diff_months_txns >= num_diff_month - 3) | (num_diff_months_txns >= min_salary_cnt_allowed)) & (
                        num_diff_months_txns > 1) & (each_recurring_credit_df.shape[0] <= num_diff_months_txns + 3):
                    
                    salary_cnt_last_6months = each_recurring_credit_df[each_recurring_credit_df['date']>=last_6month_threshold].shape[0]
                    if salary_cnt_last_6months >= 2:
                        recurring_subgroups_list += get_recurring_subgroups(each_recurring_credit_df)
            
            for each_subgroup in recurring_subgroups_list:
                each_recurring_subgroup_df = pd.DataFrame(each_subgroup)
                each_recurring_subgroup_df = each_recurring_subgroup_df.assign(calculation_method='recurring')
                salary_checks = {}
                salary_checks['num_txns'] = each_recurring_subgroup_df.shape[0]
                salary_checks['num_diff_months_txns'] = each_recurring_subgroup_df['month_year'].nunique()
                salary_checks['latest_salary_date'] = each_recurring_subgroup_df.date.max(
                )
                salary_checks['latest_salary_amount'] = each_recurring_subgroup_df[
                    each_recurring_subgroup_df['date'] == salary_checks['latest_salary_date']].amount.max()
                salary_checks['transaction_list'] = each_recurring_subgroup_df.to_dict(
                    'records')
                salary_checks['std_amount'] = \
                    each_recurring_subgroup_df.groupby('month_year')['amount'].sum().reset_index()[
                    'amount'].std()
                salary_checks['median_amount'] = each_recurring_subgroup_df["amount"].median(
                )
                salary_checks['sum_amount'] = each_recurring_subgroup_df["amount"].sum(
                )
                salary_checks['avg_monthly_salary_amount'] = salary_checks['sum_amount'] / salary_checks[
                    'num_diff_months_txns']
                    
                subgroup_salary_cnt_last_6months = each_recurring_subgroup_df[each_recurring_subgroup_df['date']>=last_6month_threshold].shape[0]
                
                if subgroup_salary_cnt_last_6months >= 3:
                    possible_salaries.append(salary_checks)
                    
                if subgroup_salary_cnt_last_6months==2:
                    possible_salaries_with_2credits.append(salary_checks)
            
            return_data = dict()
            confidence_percentage = 50
            if len(possible_salaries) > 0:
                return_data = get_best_group_from_possible_salaries(possible_salaries, month_list, standard_deviation_limit, confidence_percentage, num_diff_month,recurring_credits_list,min_salary_amount=min_salary_amount_keyword_method)
                return return_data
            
            if len(possible_salaries_with_2credits) > 0:
                return_data = get_best_group_from_possible_salaries_with_2credits(possible_salaries_with_2credits, month_list, standard_deviation_limit, confidence_percentage, num_diff_month)
                return return_data
            
            else:
                return dict()
        
        else:
            return dict()
    
    return dict()


def get_salary_transactions(transactions,employer_names=[],recurring_salary_flag=False,salary_mode = 'HARD', probable_salary_transaction_groups = [], salary_configuration = {}):
    standard_deviation_limit, min_salary_cnt_allowed, min_salary_amount, min_salary_amount_keyword_method = extract_salary_variables(salary_mode, salary_config = salary_configuration)

    month_list = get_month_list_from_transactions(transactions)
    last_6month_threshold = (datetime.strptime(month_list[-1], "%b-%Y") - relativedelta(months=5))
    #Remove None values from employer names
    employer_names = [i for i in employer_names if i is not None]
    all_transactions = pd.DataFrame(transactions)
    if 'chq_num' in all_transactions.columns:
        all_transactions['chq_num'] = all_transactions['chq_num'].fillna('')
    num_diff_month = get_num_diff_month(all_transactions)
    df = get_credit_df(transactions, min_salary_amount_keyword_method)
    if df is not None:
        df['employer_name'] = ''
        if 'salary_confidence_percentage' in df.columns:
            df['salary_confidence_percentage'] = df['salary_confidence_percentage'].fillna(0.0)
        df = df.fillna('')
    complete_credit_df = get_complete_credit_df(transactions, min_salary_amount)
    if complete_credit_df is not None:
        if 'salary_confidence_percentage' in complete_credit_df.columns:
            complete_credit_df['salary_confidence_percentage'] = complete_credit_df['salary_confidence_percentage'].fillna(0.0)
        complete_credit_df = complete_credit_df.fillna('')
    if complete_credit_df is not None:
        complete_credit_df['employer_name'] = ''
    # confidence_percentage=0
    recurring_subgroups_list = []
    has_debit_to_employer = False

    if df is not None or complete_credit_df is not None:
        df_salary = pd.DataFrame()
        if df is not None:
            df=df.fillna('')
            df_salary = df[df['transaction_channel'] == 'salary']
            df = df[(df['amount']>min_salary_amount) &
                    (~df['transaction_note'].str.contains('UPI', flags=re.IGNORECASE))]
            if 'salary_calculation_method' in df_salary.columns:
                '''
                    df_salary is the dataframe including transctions that have been marked as salary with keyword. Therefore, excluding recurring and employer_name.
                    employer_name and recurring salary transactions are anyways again calculated below in employer_salary_df & salary_df.
                '''
                df_salary = df_salary[
                    (df_salary['salary_calculation_method'] != 'recurring') &
                    (df_salary['salary_calculation_method'] != 'employer_name')
                ]
            # Assigning calculation_method equal to keyword for those transactions that have been classified to salary by keyword in fsmlib.
            df_salary = df_salary.assign(calculation_method='keyword')

        if df is not None and df.shape[0] == 0:
            return dict()
        
        employer_salary_df = pd.DataFrame()
        if len(employer_names) > 0 and complete_credit_df is not None:
            employer_salary_df = get_salary_from_employer_names(complete_credit_df,employer_names)
            employer_salary_df = employer_salary_df.assign(calculation_method='employer_name')

        keyword_salary_seen_last3months = False if len(df_salary)==0 else (datetime.today() - pd.to_datetime(df_salary['date'].max())).days < 90
        # Recurring Salary 1(Keyword + Recurring)
        recurring_subgroup = []
        if len(employer_salary_df) == 0:
            top2_transaction_each_month = get_topk_credit_transactions_each_month(complete_credit_df,2)
            if len(top2_transaction_each_month)>0:
                median_top_salary = top2_transaction_each_month['amount'].median()
                # Cluster based on top salary
                cluster_top_salaries = get_top_salary_cluster(df=top2_transaction_each_month,match_threshold=90)

                for each_cluster in cluster_top_salaries:
                    # Debit Check
                    get_all_transaction_with_match = get_all_transaction_fuzz_score(transactions,each_cluster[0],fuzz_threshold=93)
                    probable_transaction =pd.DataFrame(get_all_transaction_with_match)
                    probable_debit_df = probable_transaction[probable_transaction['transaction_type'] == 'debit']
                    probable_debit_df = get_debit_transaction(probable_debit_df)


                    probable_transaction = probable_transaction[(probable_transaction['transaction_type'] == 'credit')]
                    probable_transaction,fail_check = keyword_check(probable_transaction) 

                    # Don't consider if more than 1 debit
                    if (probable_debit_df is None or probable_debit_df.shape[0]<2) and probable_transaction.shape[0]>1 and fail_check.shape[0]<1:
                        # Group by month and count the number of credits
                        probable_transaction['date']=pd.to_datetime(probable_transaction['date'])
                        probable_transaction['month'] = probable_transaction['date'].dt.strftime('%Y-%m')
                        credit_counts = probable_transaction.groupby('month').size()

                        # Boolean: More than 3 credits in a single month?
                        more_than_3_credits_in_a_month = (credit_counts > 3).any()

                        # Boolean: More than 2 credits in multiple months?
                        more_than_2_credits_in_multiple_months = (credit_counts > 2).sum() > 1

                        # Unique salary month
                        unique_salary_months = probable_transaction['month'].nunique()
                        get_subgroup = get_recurring_subgroups(df=probable_transaction,amt_deviation=0.2,date_deviation=5) 

                        if (not more_than_3_credits_in_a_month) and (not more_than_2_credits_in_multiple_months) and (unique_salary_months>=3) and (len(get_subgroup)==1) and (len(get_subgroup)>=3):
                            recurring_subgroups_list += get_subgroup   
                        
                if len(recurring_subgroups_list)!=0:
                    recent_recurring_group = []
                    most_recent_date = None
                    for recurring_group in recurring_subgroups_list:
                        sublist_most_recent_date = max([datetime.strptime(str(item['date']), '%Y-%m-%d %H:%M:%S') for item in recurring_group])
                        if not most_recent_date or sublist_most_recent_date > most_recent_date:
                            most_recent_date = sublist_most_recent_date
                            recent_recurring_group = recurring_group
                    recurring_subgroup = recent_recurring_group


            df_recurring_salary = pd.DataFrame(recurring_subgroup)
            df_recurring_salary = df_recurring_salary.assign(calculation_method='recurring')

        if keyword_salary_seen_last3months is False and len(employer_salary_df) == 0:
            employer_salary_keyword_df =pd.concat([df_salary,df_recurring_salary])
        else:
            employer_salary_keyword_df =pd.concat([employer_salary_df,df_salary])
        
        employer_salary_keyword_df = employer_salary_keyword_df.drop_duplicates(subset=employer_salary_keyword_df.columns.difference(['calculation_method','optimizations']))
        employer_salary_keyword_df.reset_index(drop=True, inplace=True)

        if len(employer_salary_keyword_df) > 0:
            return_data = {}
            num_diff_month = get_num_diff_month(employer_salary_keyword_df)
            employer_salary_keyword_df = get_salary_processed_data(employer_salary_keyword_df, month_list, standard_deviation_limit, ignore_deviation=True)
            if employer_salary_keyword_df.empty:
                return dict()
            # employer_salary_keyword_df['date'] = employer_salary_keyword_df['date'].dt.strftime('%Y-%m-%d')
            salary_transactions =employer_salary_keyword_df.to_dict('records')
            # salary_transactions = sorted(salary_transactions, key=lambda x: x['date'])
            cnt_salary_months = employer_salary_keyword_df['salary_month'].nunique()
            return_data['salary_transactions'] = salary_transactions
            return_data['avg_monthly_salary_amount'] = (employer_salary_keyword_df.amount.sum() / cnt_salary_months) if cnt_salary_months > 0  else 0.0
            return_data['num_salary_months'] = cnt_salary_months
            return_data['latest_salary_date'] = employer_salary_keyword_df.date.max()
            return_data['latest_salary_amount'] = employer_salary_keyword_df[employer_salary_keyword_df['date'] == return_data['latest_salary_date']].amount.max()
            return_data['confidence_percentage'] = 100
            return return_data
        elif recurring_salary_flag and df is not None: 
            recurring_credits_list = probable_salary_transaction_groups
            possible_salaries = []
            possible_salaries_with_2credits = []
            recurring_subgroups_list = []

            # Existing Logic
            for each_recurring_credit in recurring_credits_list:
                each_recurring_credit_df_all_amount = pd.DataFrame(each_recurring_credit)
                each_recurring_credit_df = each_recurring_credit_df_all_amount[each_recurring_credit_df_all_amount['amount'] > min_salary_amount]
                # TODO remove random columns from each_recurring_credit_df
                num_diff_months_txns = get_num_diff_month(each_recurring_credit_df)

                # Debit check
                get_all_transaction_with_match = get_all_transaction_fuzz_score(transactions,each_recurring_credit[0],fuzz_threshold=93)
                probable_transaction =pd.DataFrame(get_all_transaction_with_match)
                probable_debit_df = probable_transaction[probable_transaction['transaction_type'] == 'debit']
                probable_debit_df = get_debit_transaction(probable_debit_df)

                # Fp Handling Code
                if len(top2_transaction_each_month)>0:
                    each_recurring_credit_df, fail_check = keyword_check(each_recurring_credit_df)
                    if fail_check.shape[0] != 0:
                        fp_handling = transaction_is_top_transaction_of_month(top2_transaction_each_month, fail_check, median_top_salary, 0.2)
                        if (fp_handling.shape[0]!=0):
                            each_recurring_credit_df = pd.concat([each_recurring_credit_df,fp_handling])
                
                has_debit_to_employer = 0 if probable_debit_df is None else probable_debit_df.shape[0]
                if (each_recurring_credit_df.shape[0] > 1) & ((num_diff_months_txns >= num_diff_month - 3) | (num_diff_months_txns >= min_salary_cnt_allowed)) & (
                        num_diff_months_txns > 1) & (each_recurring_credit_df.shape[0] <= num_diff_months_txns + 3) & (probable_debit_df is None or probable_debit_df.shape[0]<=2):
                    salary_cnt_last_6months = each_recurring_credit_df[each_recurring_credit_df['date']>=last_6month_threshold].shape[0]
                    if salary_cnt_last_6months >= 2:
                        recurring_subgroups_list += get_recurring_subgroups(each_recurring_credit_df)
            
            # Run with fuzz score>=70
            if len(recurring_subgroups_list) == 0 and len(top2_transaction_each_month)>0:
                recurring_credits_list2 = get_recurring_using_dict_with_fuzz_threshold(df,70)
                for each_recurring_credit in recurring_credits_list2:
                    each_recurring_credit_df_all_amount = pd.DataFrame(each_recurring_credit)
                    each_recurring_credit_df = each_recurring_credit_df_all_amount[each_recurring_credit_df_all_amount['amount'] > min_salary_amount]
                    
                    each_recurring_credit_df,fail_check = keyword_check(each_recurring_credit_df)

                    each_recurring_credit_df = transaction_is_top_transaction_of_month(top2_transaction_each_month,each_recurring_credit_df,median_top_salary,0.1)
                    if fail_check.shape[0] != 0:
                        fp_handling = transaction_is_top_transaction_of_month(top2_transaction_each_month, fail_check, median_top_salary, 0.05)
                        recurring_credit_df = pd.concat([each_recurring_credit_df, fp_handling])
                    else:
                        recurring_credit_df = each_recurring_credit_df
                    
                    if recurring_credit_df.shape[0]>1:
                        num_diff_months_txns = get_num_diff_month(recurring_credit_df)
                        # Debit check
                        get_all_transaction_with_match = get_all_transaction_fuzz_score(transactions,each_recurring_credit[0],fuzz_threshold=93)
                        probable_transaction =pd.DataFrame(get_all_transaction_with_match)
                        probable_debit_df = probable_transaction[probable_transaction['transaction_type'] == 'debit']
                        probable_debit_df = get_debit_transaction(probable_debit_df)
                        has_debit_to_employer = 0 if probable_debit_df is None else probable_debit_df.shape[0]
                        
                        # Additional Checks
                        probable_transaction['date']=pd.to_datetime(probable_transaction['date'])
                        recurring_credit_df['month'] = recurring_credit_df['date'].dt.strftime('%Y-%m')
                        credit_counts = recurring_credit_df.groupby('month').size()

                        # Boolean: More than 3 credits in a single month?
                        more_than_3_credits_in_a_month = (credit_counts > 3).any()

                        # Boolean: More than 2 credits in multiple months?
                        more_than_2_credits_in_multiple_months = (credit_counts > 2).sum() > 1

                        # Unique salary month
                        unique_salary_months = recurring_credit_df['month'].nunique()

                        if (probable_debit_df is None or len(probable_debit_df)<3) and (not more_than_3_credits_in_a_month) and (not more_than_2_credits_in_multiple_months) and (unique_salary_months>=2):
                            recurring_subgroups_list += get_recurring_subgroups(recurring_credit_df,0.1,10)
            
            # # Run with fuzz score>=50
            if len(recurring_subgroups_list) == 0 and len(top2_transaction_each_month)>0:
                recurring_credits_list2 = get_recurring_using_dict_with_fuzz_threshold(df,50)
                for each_recurring_credit in recurring_credits_list2:
                    each_recurring_credit_df_all_amount = pd.DataFrame(each_recurring_credit)
                    each_recurring_credit_df = each_recurring_credit_df_all_amount[each_recurring_credit_df_all_amount['amount'] > min_salary_amount]
                    each_recurring_credit_df,fail_check = keyword_check(each_recurring_credit_df)

                    each_recurring_credit_df = transaction_is_top_transaction_of_month(top2_transaction_each_month,each_recurring_credit_df,median_top_salary,0.05)
                    recurring_credit_df = each_recurring_credit_df  

                    if recurring_credit_df.shape[0]>1:
                        num_diff_months_txns = get_num_diff_month(recurring_credit_df)
                        # Debit check
                        get_all_transaction_with_match = get_all_transaction_fuzz_score(transactions,each_recurring_credit[0],fuzz_threshold=93)
                        probable_transaction =pd.DataFrame(get_all_transaction_with_match)
                        probable_debit_df = probable_transaction[probable_transaction['transaction_type'] == 'debit']
                        probable_debit_df = get_debit_transaction(probable_debit_df)
                        has_debit_to_employer = 0 if probable_debit_df is None else probable_debit_df.shape[0]

                        # Additional Checks
                        recurring_credit_df['date']=pd.to_datetime(recurring_credit_df['date'])
                        recurring_credit_df['month'] = recurring_credit_df['date'].dt.strftime('%Y-%m')
                        credit_counts = recurring_credit_df.groupby('month').size()

                        # Boolean: More than 3 credits in a single month?
                        more_than_3_credits_in_a_month = (credit_counts > 3).any()

                        # Boolean: More than 2 credits in multiple months?
                        more_than_2_credits_in_multiple_months = (credit_counts > 2).sum() > 1

                        # Unique salary month
                        unique_salary_months = recurring_credit_df['month'].nunique()

                        if (probable_debit_df is None or len(probable_debit_df)<3) and (not more_than_3_credits_in_a_month) and (not more_than_2_credits_in_multiple_months) and (unique_salary_months>=2):
                            recurring_subgroups_list += get_recurring_subgroups(recurring_credit_df,0.1,10)

            for each_subgroup in recurring_subgroups_list:
                each_recurring_subgroup_df = pd.DataFrame(each_subgroup)
                each_recurring_subgroup_df['month_year'] = each_recurring_subgroup_df['date'].apply(lambda x: get_salary_month(x))
                each_recurring_subgroup_df = each_recurring_subgroup_df.assign(calculation_method='recurring')
                salary_checks = {}
                salary_checks['num_txns'] = each_recurring_subgroup_df.shape[0]
                salary_checks['num_diff_months_txns'] = each_recurring_subgroup_df['month_year'].nunique()
                salary_checks['latest_salary_date'] = each_recurring_subgroup_df.date.max()
                salary_checks['latest_salary_amount'] = each_recurring_subgroup_df[
                    each_recurring_subgroup_df['date'] == salary_checks['latest_salary_date']].amount.max()
                salary_checks['transaction_list'] = each_recurring_subgroup_df.to_dict(
                    'records')
                salary_checks['std_amount'] = \
                    each_recurring_subgroup_df.groupby('month_year')['amount'].sum().reset_index()[
                    'amount'].std()
                salary_checks['median_amount'] = each_recurring_subgroup_df["amount"].median(
                )
                salary_checks['sum_amount'] = each_recurring_subgroup_df["amount"].sum(
                )
                salary_checks['avg_monthly_salary_amount'] = salary_checks['sum_amount'] / salary_checks[
                    'num_diff_months_txns']
                    
                subgroup_salary_cnt_last_6months = each_recurring_subgroup_df[each_recurring_subgroup_df['date']>=last_6month_threshold].shape[0]
                
                if subgroup_salary_cnt_last_6months >= 3:
                    possible_salaries.append(salary_checks)
                    
                if subgroup_salary_cnt_last_6months==2:
                    possible_salaries_with_2credits.append(salary_checks)
            
            confidence_percentage = 50
            if len(possible_salaries) > 0:
                return_data = new_confidence_calculator(possible_salaries, month_list, standard_deviation_limit, confidence_percentage, num_diff_month, has_debit_to_employer)
                return return_data
                # if len(possible_salaries) > 0:
                #     return_data = get_best_group_from_possible_salaries(possible_salaries, month_list, standard_deviation_limit, confidence_percentage, num_diff_month)
                #     return return_data
                
            if len(possible_salaries_with_2credits) > 0:
                return_data = get_best_group_from_possible_salaries_with_2credits(possible_salaries_with_2credits, month_list, standard_deviation_limit, confidence_percentage, num_diff_month)
                return return_data
            else:
                return dict()
        
        else:
            return dict()
    
    return dict()

def check_neft_rtgs_credits(salary_df):
    for ind in salary_df.index:
        txn_note = salary_df['transaction_note'][ind]
        if isinstance(txn_note, str):
            txn_note = txn_note.upper()
        if 'NEFT' not in txn_note and 'RTGS' not in txn_note:
            return False
        
    return True

def check_company_keywords(salary_df):
    
    company_pattern = '.*PVT.*|.*LTD.*|.*PRIVATE.*|.*LIMITED.*|.*TECH.*|.*SOLU.*|.*SERV.*|.*CONSULT.*|.*INDUST.*|.*ENTERPRISE.*|.*EXPOR.*|.*TRADER.*|.*MANAGEMENT.*|.*LOGISTIC.*|.*HOTEL.*|.*INTERNATIONAL.*|.*HOSPITAL.*'
    compiled_pattern = re.compile(company_pattern)
    
    for ind in salary_df.index:
        txn_note = salary_df['transaction_note'][ind]
        if isinstance(txn_note, str):
            txn_note = txn_note.upper()
            result = re.match(compiled_pattern, txn_note)
            if not result:
                return False
        else:
            return False
        
    return True

def check_end_of_month_salary(salary_df):
    for ind in salary_df.index:
        salary_day = salary_df['date'][ind].day
        if 6 < salary_day < 24:
            return False
    return True

def check_salary_amount_within_deviation_limit(salary_df):
    allowed_deviation = 0.2
    median_amount = salary_df['amount'].median()
    
    salary_df_within_20perc_deviation = salary_df[(salary_df['amount'] > median_amount*(1-allowed_deviation)) & (salary_df['amount'] < median_amount*(1+allowed_deviation))]
    if len(salary_df_within_20perc_deviation) == len(salary_df):
        return True
    else:
        return False

def check_credit_date_deviation(salary_df):
    first_txn_credit_day = salary_df['date'].min().day
    
    for ind in salary_df.index:
        credit_day = salary_df['date'][ind].day
        abs_days_diff = abs(first_txn_credit_day - credit_day)
        if 4 < abs_days_diff < 26:
            return False
        
    return True

def extract_salary_variables(salary_mode, salary_config = {}):
    standard_deviation_limit = salary_config.get('standard_deviation_limit', 0.4) if isinstance(salary_config,dict) else 0.4   # for HARD and EASY case
    min_salary_cnt_allowed = salary_config.get('min_salary_cnt_allowed', 3) if isinstance(salary_config,dict) else 3
    min_salary_amount = salary_config.get('min_salary_amount', 5000) if isinstance(salary_config,dict) else 5000
    min_salary_amount_keyword_method = salary_config.get('min_salary_amount_keyword_method', 3000) if isinstance(salary_config,dict) else 3000
    
    return standard_deviation_limit, min_salary_cnt_allowed, min_salary_amount, min_salary_amount_keyword_method


def check_gap_salary_txns(salary_transaction_list: list,salary_mode = 'HARD'):
    # this method checks that the subsequent
    # txns should have a gap of atleast 25 days and max 35 days for HARD mode and 20,40 resp.. for easy mode
    min_gap_days = 25
    max_gap_days = 35
    trans_per_exception_limit = 6
    if salary_mode == 'EASY':
        min_gap_days = 20
        max_gap_days = 40
        trans_per_exception_limit = 4

    # sort by date - desc
    salary_transactions_df = pd.DataFrame(salary_transaction_list)
    salary_transactions_df = salary_transactions_df.sort_values(
        by="date", ascending=False)

    # print(salary_transactions_df.to_markdown())

    total_txns = salary_transactions_df.shape[0]

    exceptions_count = 0
    month_set_covered = set()

    # print("total: ", total_txns)
    for i in range(0, total_txns - 1):
        # i-th row
        curr_row = salary_transactions_df.iloc[i, :]
        # (i+1)-th row
        next_row = salary_transactions_df.iloc[i + 1, :]

        gap_delta = curr_row.date - next_row.date

        month_flag_left = str(curr_row.date.month)+"_"+str(curr_row.date.year)
        month_flag_right = str(next_row.date.month)+"_"+str(next_row.date.year)
        if ( gap_delta.days < min_gap_days or gap_delta.days > max_gap_days ) and (
            month_flag_left not in month_set_covered and month_flag_right not in month_set_covered ): 
            exceptions_count += 1
            month_set_covered.add(month_flag_left)
            month_set_covered.add(month_flag_right)

    # print("Exceptions: ", exceptions_count)
    # print("Exceptions Error: ", exceptions_count / total_txns)

    if exceptions_count > 0:
        # allowing 16.66% error for hard mode and 25% for easy mode
        return True if exceptions_count/total_txns <= (1/trans_per_exception_limit) else False

    return True

def get_credit_df(transactions, min_salary_amount: int):
    df = pd.DataFrame(transactions)
    if df.shape[0] > 0:
        channel_allowed_list = ['net_banking_transfer', 'salary', 'Other', 'other']
        ignore_salary_patterns = ".*early\s*salary.*|.*esloan.*|.*flex\s*salary.*|.*cash\s*loan.*|.*loan\s*disb.*"
        credit_df = df[(df['transaction_type'] == 'credit') & (df['amount'] > min_salary_amount) & 
        ( df['transaction_channel'].isin(channel_allowed_list) ) &
            # ( df['merchant_category'] != 'loans') & (df['description'] != 'lender_transaction')
            (~df["transaction_note"].str.contains(ignore_salary_patterns, flags=re.IGNORECASE))
            ]
        if credit_df.shape[0] > 0:
            return credit_df
        else:
            return None
    else:
        return None

def get_complete_credit_df(transactions, min_salary_amount: int):
    df = pd.DataFrame(transactions)
    if df.shape[0] > 0:
        credit_df = df[(df['transaction_type'] == 'credit') & (df['amount'] > min_salary_amount)]
        if credit_df.shape[0] > 0:
            return credit_df
        else:
            return None
    else:
        return None


def add_column_contains_salary(df):
    pattern = '(salary|slry| sal |payroll|/sal)'
    pattern_2 = '(PVT|LTD|PRIVATE|LIMITED|TECH|SOLU|SERV)'
    imps_pattern = '(Imps)'
    neft_pattern = '(NEFT)'
    df = df.assign(Calc_containsSalary=False)
    df['Calc_containsSalary'] = df['transaction_note'].str.contains(
        pattern, flags=re.IGNORECASE)
    df['Calc_contains_company'] = df['transaction_note'].str.contains(
        pattern_2, flags=re.IGNORECASE)
    df['Calc_contains_imps'] = df['transaction_note'].str.contains(
        imps_pattern, flags=re.IGNORECASE)
    df['Calc_contains_neft'] = df['transaction_note'].str.contains(
        neft_pattern, flags=re.IGNORECASE)
    return df


def get_num_diff_month(df):
    # df['date'] = df['date'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d'))
    df['date'] = df['date'].apply(
        lambda x: datetime.strptime(str(x), '%Y-%m-%d %H:%M:%S'))
    if df.shape[0] == 0:
        return 0
    df['month_year'] = df['date'].apply(
        lambda x: get_month_year_for_salary_txn(x))
    date_list = df['month_year'].unique().tolist()
    num_diff_months = len(date_list)
    return num_diff_months


def get_month_year_for_salary_txn(date):
    if date.day > 25:
        date = date + timedelta(days=10)
    return str(date.month) + '-' + str(date.year)


def choose_best_salary(df,salary_mode = 'HARD'):
    standard_deviation_limit = 0.25   # for HARD case
    if salary_mode == 'EASY':
        standard_deviation_limit = 0.3
    # TODO check the number for diff months available 1st
    if df[df['num_salary'] > 0].shape[0] > 0:
        max_salary_row = df['num_salary'].idxmax()
        return df.iloc[[max_salary_row]].to_dict('records')[0]
    else:
        df = df.sort_values(by='avg_monthly_salary_amount', ascending=False)
        # for first place
        best_salary_0 = df.iloc[0, :]
        std_dev_0 = best_salary_0['std_amount']
        avg_monthly_salary_amount_0 = best_salary_0['avg_monthly_salary_amount']

        # for second place
        best_salary_1 = df.iloc[1, :]
        std_dev_1 = best_salary_1['std_amount']
        avg_monthly_salary_amount_1 = best_salary_1['avg_monthly_salary_amount']

        if std_dev_0/avg_monthly_salary_amount_0 > standard_deviation_limit:
            if std_dev_1/avg_monthly_salary_amount_1 > standard_deviation_limit:
                return None
            else:
                # check if this recurring txns follow allowed dates delta
                return best_salary_1 if check_gap_salary_txns(best_salary_1["transaction_list"],salary_mode) else None
        else:
            # check if this recurring txns follow allowed dates delta
            return best_salary_0 if check_gap_salary_txns(best_salary_0["transaction_list"],salary_mode) else None

        # max_salary_row = df['avg_monthly_salary_amount'].idxmax()
        # print max_salary_row
        # print df
        # return df.iloc[[max_salary_row]].to_dict('records')[0]
    # elif df[df['num_company'] > 0].shape[0] > 0:
    #     choose_salary_level1(df[df['num_company'] > 0])
    #     max_company_row = df['num_company'].idxmax()
    #     return df.iloc[[max_company_row]].to_dict('records')[0]
    # elif df[df['num_neft'] > 0].shape[0] > 0:
    #     max_neft_row = df['num_neft'].idxmax()
    #     return df.iloc[[max_neft_row]].to_dict('records')[0]
    # elif df[df['num_imps'] > 0].shape[0] > 0:
    #     max_imps_row = df['num_imps'].idxmax()
    #     return df.iloc[[max_imps_row]].to_dict('records')[0]
    # else:
    #     return None

# def get_salary_transactions(transactions):
#     df = get_credit_df(transactions)
#     if df is not None:
#         df = add_salary_related_columns(df)
#         df['date'] = df['date'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
#         df_dict = df.to_dict('records')
#         salary_meta_dict = get_best_salary(df_dict)
#         if salary_meta_dict is not None:
#             confidence = get_probability_salary(salary_meta_dict)
#             salary_df = salary_meta_dict['df'][['amount', 'balance', 'date', 'transaction_note', 'transaction_channel']]
#             salary_df['transaction_type'] = 'credit'
#             return salary_df.to_dict('records')
#         else:
#             return list()
#     else:
#         return list()
#
#
#
# def add_salary_related_columns(df):
#     df = add_column_contains_salary(df)
#     df['transaction_channel'] = df.apply(lambda x: transaction_channel_process(x), axis=1)
#     df = add_amount_bin(df).sort_values(by=['date', 'amount'], ascending=True)
#     return df
#
#
# def transaction_channel_process(x):
#     list_of_neft = ['NEFT']
#     list_of_chq = ['CHEQUE', 'CHQ']
#     list_of_upi = ['UPI']
#     list_of_csh_wdl = ['CASH DEP', 'CSH DEP']
#     if process.extractOne(x['transaction_note'].upper(), list_of_neft, scorer=fuzz.partial_ratio, score_cutoff=100):
#         return 'NEFT'
#     elif process.extractOne(x['transaction_note'].upper(), list_of_chq, scorer=fuzz.partial_ratio, score_cutoff=100):
#         return 'CHEQUE'
#     elif process.extractOne(x['transaction_note'].upper(), list_of_upi, scorer=fuzz.partial_ratio, score_cutoff=100):
#         return 'UPI'
#     elif process.extractOne(x['transaction_note'].upper(), list_of_csh_wdl, scorer=fuzz.partial_ratio,
#                             score_cutoff=100):
#         return 'CASH DEPOSIT'
#     else:
#         return None
#
#
# def add_amount_bin(df):
#     df = df.assign(amount_bin=df.amount.apply(lambda x: x // AMOUNT_BIN_SIZE))
#     return df
#
#
# def get_best_salary(df_dict):
#     final_dict = {}
#     for each in df_dict:
#         similar_transactions = list()
#         similar_transactions.append(each)
#         for every in df_dict:
#             condition_1 = (abs(each['amount_bin'] - every['amount_bin']) < 3)
#             condition_2 = (abs(each['date'].day - every['date'].day) < 4)
#             condition_3 = (abs(each['date'].day - every['date'].day) > 24)
#             condition_4 = (abs(each['date'] - every['date']).days > 20)
#             if condition_1 and (condition_2 or condition_3) and condition_4:
#                 similar_transactions.append(every)
#
#         # TODO make sure not to remove salary dict, if both has salary keep the one with less amount diff
#         new_df = []
#         for i in range(0, len(similar_transactions)):
#             if i + 1 < len(similar_transactions):
#                 if abs((similar_transactions[i]['date'] - similar_transactions[i + 1]['date']).days) > 20:
#                     new_df.append(similar_transactions[i])
#             else:
#                 new_df.append(similar_transactions[i])
#
#         group_salaries = pd.DataFrame(new_df)
#         num_salary_available = group_salaries[group_salaries['Calc_containsSalary'] == True].shape[0]
#         num_companies_available = group_salaries[group_salaries['Calc_contains_company'] == True].shape[0]
#         num_companies_non_salaries_available = group_salaries[
#             (group_salaries['Calc_containsSalary'] == False) & (group_salaries['Calc_contains_company'] == True)].shape[
#             0]
#         result_dict = dict()
#         result_dict['df'] = group_salaries
#         result_dict['num_transactions'] = group_salaries.shape[0]
#         result_dict['num_salary_available'] = num_salary_available
#         result_dict['num_companies_available'] = num_companies_available
#         result_dict['num_companies_non_salaries_available'] = num_companies_non_salaries_available
#         result_dict['tota_salary_company'] = num_companies_non_salaries_available + num_salary_available
#
#         if final_dict == {}:
#             if (result_dict['tota_salary_company'] == 0) and (result_dict['num_transactions'] == 1):
#                 continue
#             else:
#                 final_dict = result_dict
#         else:
#             condition_replace_1 = final_dict['num_transactions'] < result_dict['num_transactions']
#             condition_replace_2 = final_dict['tota_salary_company'] < result_dict['tota_salary_company']
#
#             if condition_replace_1:
#                 if condition_replace_2:
#                     print('existing has less salary + company transactions and less number of transactions')
#                     final_dict = result_dict
#                 elif not condition_replace_2:
#                     num_transaction = result_dict['num_transactions']
#                     num_salary_transact = result_dict['num_salary_available']
#                     num_companies_non_salaries_available = result_dict['num_companies_non_salaries_available']
#                     if (num_salary_transact + num_companies_non_salaries_available) / num_transaction > 0.75:
#                         print(
#                             'New set has more number of transactions and enough number of salary + company transactions')
#                         final_dict = result_dict
#                     else:
#                         print(
#                             'New set has more number of transactions but not enough number of salary + company transactions')
#             elif not condition_replace_1:
#                 if condition_replace_2:
#                     previous_num_transaction = final_dict['num_transactions']
#                     new_num_transaction = result_dict['num_transactions']
#                     if new_num_transaction / previous_num_transaction > 0.75:
#                         print('new has sufficient number of transaction and more salary transaction')
#                         final_dict = result_dict
#                     else:
#                         print('new has more salary transaction but not sufficient number of transaction')
#                 elif not condition_replace_2:
#                     print('existing has more or equal salary and more or equal transaction')
#
#     if final_dict == {}:
#         return None
#     else:
#         return final_dict
#
#
# def get_probability_salary(salary_meta_dict):
#     salary_present_weight = 0.4
#     salary_present_confidence = float(salary_meta_dict['tota_salary_company']) / float(
#         salary_meta_dict['num_transactions'])
#
#     num_continuous_salary_month = get_num_diff_month(salary_meta_dict['df'])
#     diff_months_available = salary_meta_dict['df'].shape[0]
#
#     continuous_data_weight = 0.4
#     continuous_data_confidence = float(num_continuous_salary_month) / float(diff_months_available)
#
#     if num_continuous_salary_month > 2:
#         num_continuous_salary_month_confidence = 1
#     else:
#         num_continuous_salary_month_confidence = 0.3
#
#     num_continuous_salary_month_weight = 0.2
#
#     a = float(salary_present_weight) * float(salary_present_confidence)
#
#     b = float(continuous_data_weight) * float(continuous_data_confidence)
#
#     final_confidence = a + b + float(num_continuous_salary_month_weight) * float(num_continuous_salary_month_confidence)
#     return float(final_confidence)


def get_month_list_from_transactions(transactions):
    month_list = []
    
    if len(transactions)>0:
        from_date = datetime.strptime(transactions[0].get("date"),"%Y-%m-%d %H:%M:%S")
        to_date = datetime.strptime(transactions[-1].get("date"),"%Y-%m-%d %H:%M:%S")
        temp_date = from_date
        
        month_list.append(temp_date.strftime("%b-%Y"))
        while(temp_date.strftime("%b-%Y")!=to_date.strftime("%b-%Y") and temp_date<to_date):
            temp_date = temp_date + relativedelta(months=1)
            month_list.append(temp_date.strftime("%b-%Y"))

    return month_list


def get_salary_month(credit_date):
    
    if credit_date.day < DATE_THRESHOLD:
        salary_month = (credit_date.date() - timedelta(days = credit_date.day)).strftime("%b-%Y")
    else:
        salary_month = credit_date.strftime("%b-%Y")
        
    return salary_month


def get_salary_processed_data(salary_df, month_list, standard_deviation_limit, ignore_deviation=False,median_salary_amount=None):
    if salary_df.empty:
        return pd.DataFrame()
    salary_df.reset_index(drop=True, inplace=True)
    salary_df['date'] = pd.to_datetime(salary_df['date'])
    salary_df.sort_values(by='date', inplace=True)
    if median_salary_amount is None:
        median_salary_amount = salary_df.tail(6)['amount'].median()
    salary_df['salary_month'] = salary_df['date'].apply(lambda x: get_salary_month(x))
    salary_df['delta'] = abs((median_salary_amount - salary_df['amount'])/ median_salary_amount) if median_salary_amount > 0 else 1

    if not ignore_deviation:
        salary_df = salary_df[salary_df['delta'] < standard_deviation_limit]
    
    previous_month = None
    salary_months_available = list(salary_df['salary_month'].unique())
    for month_key in month_list:
        month_salary_df = salary_df[salary_df['salary_month']==month_key]

        if len(month_salary_df) > 1:
            if previous_month and previous_month not in salary_months_available:
                
                ### Update salary_month by shifting to previous month
                index_tmp = month_salary_df.index[0]
                salary_df.loc[index_tmp, 'salary_month'] = previous_month
                salary_months_available = list(salary_df['salary_month'].unique())

            else:
                eligible_salary_amounts_index = list(month_salary_df.index)
                best_eligible_salary_amounts_index = (month_salary_df[month_salary_df['delta'] < 0.3]['delta'].idxmin() 
                                    if (month_salary_df['delta'] < 0.3).any() 
                                    else month_salary_df['amount'].idxmax())
                eligible_salary_amounts_index.remove(best_eligible_salary_amounts_index)
                ### Update salary_month
                salary_df.loc[eligible_salary_amounts_index, 'salary_month'] = None
                salary_months_available = list(salary_df['salary_month'].unique())

        previous_month = month_key

    salary_df = salary_df[~salary_df['salary_month'].isna()]
    salary_df.drop_duplicates(subset='salary_month', keep='last', inplace=True)
    
    if len(salary_df)<2 and not ignore_deviation:
        return pd.DataFrame()
    
    salary_df.drop('delta', axis=1, inplace=True)
    salary_df.reset_index(drop=True, inplace=True)
    return salary_df

def get_best_group_from_possible_salaries(possible_salaries, month_list, standard_deviation_limit, confidence_percentage, num_diff_month,recurring_credits_list,min_salary_amount=MINIMUM_PERMISSIBLE_SALARY_INR):
    return_data = dict()
    possible_salaries_df = pd.DataFrame(possible_salaries)
    possible_salaries_df = possible_salaries_df.sort_values(by=['avg_monthly_salary_amount', 'latest_salary_date'], ascending=[False, False])
    salary_transactions = possible_salaries_df['transaction_list'].iloc[0]
    median_salary_amount = possible_salaries_df['median_amount'].iloc[0]
    salary_df = pd.DataFrame(salary_transactions) 
    for recur_credit in recurring_credits_list:
        employer_hashes = set(salary_df['hash'])
        recur_credit_hashes = {rc['hash'] for rc in recur_credit}
        hash_match = bool(employer_hashes & recur_credit_hashes)
        if hash_match:
            matched = pd.DataFrame(recur_credit)
            matched = matched.assign(calculation_method='recurring')
            matched = same_employer_checks(matched)
            salary_df = pd.concat([salary_df,matched])
    salary_df = salary_df.drop_duplicates(subset=['hash'],keep='first')
    salary_df = get_credit_df(transactions=salary_df, min_salary_amount=min_salary_amount)
    if salary_df is None:
        return dict()
    salary_df = get_salary_processed_data(salary_df, month_list, standard_deviation_limit,ignore_deviation=True,median_salary_amount=median_salary_amount)
    if salary_df.empty:
        return dict()
    
    cnt_salary_months = salary_df['salary_month'].nunique()
    neft_rtgs_flag = check_neft_rtgs_credits(salary_df)
    company_name_flag = check_company_keywords(salary_df)
    end_of_month_flag = check_end_of_month_salary(salary_df)
    amount_deviation_flag = check_salary_amount_within_deviation_limit(salary_df)
    credit_date_deviation_flag = check_credit_date_deviation(salary_df)
    
    if (num_diff_month - cnt_salary_months <= 2) and (cnt_salary_months > 2):
        confidence_percentage += 10
    if end_of_month_flag:
        confidence_percentage += 10
    if company_name_flag or neft_rtgs_flag:
        confidence_percentage += 10
    if amount_deviation_flag and credit_date_deviation_flag:
        confidence_percentage += 10
    
    salary_transactions =salary_df.to_dict('records')
    return_data['salary_transactions'] = salary_transactions
    return_data['avg_monthly_salary_amount'] = possible_salaries_df['avg_monthly_salary_amount'].iloc[0]
    return_data['num_salary_months'] = salary_df['salary_month'].nunique()
    return_data['latest_salary_date'] = possible_salaries_df['latest_salary_date'].iloc[0]
    return_data['latest_salary_amount'] = possible_salaries_df['latest_salary_amount'].iloc[0]
    return_data['confidence_percentage'] = confidence_percentage
    
    return return_data

def get_best_group_from_possible_salaries_with_2credits(possible_salaries_with_2credits, month_list, standard_deviation_limit, confidence_percentage, num_diff_month):
    return_data = dict()
    possible_salaries_df = pd.DataFrame(possible_salaries_with_2credits)
    possible_salaries_df = possible_salaries_df.sort_values(by=['avg_monthly_salary_amount', 'latest_salary_date'], ascending=[False, False])
    salary_transactions = possible_salaries_df['transaction_list'].iloc[0]
    salary_df = pd.DataFrame(salary_transactions)
    
    neft_rtgs_flag = check_neft_rtgs_credits(salary_df)
    company_name_flag = check_company_keywords(salary_df)
    end_of_month_flag = check_end_of_month_salary(salary_df)
    amount_deviation_flag = check_salary_amount_within_deviation_limit(salary_df)
    credit_date_deviation_flag = check_credit_date_deviation(salary_df)
    
    if end_of_month_flag:
        confidence_percentage += 10
    if company_name_flag or neft_rtgs_flag:
        confidence_percentage += 10
    if amount_deviation_flag and credit_date_deviation_flag:
        confidence_percentage += 10
    
    if len(salary_df)==2:
        amount_1 =  salary_df['amount'].iloc[0]
        amount_2 =  salary_df['amount'].iloc[1]
        amount_variation = abs((amount_2/amount_1)-1) if amount_1>0 else 1
        amount_variation_check = amount_variation <= STANDARD_DEVITATION_LIMIT_2CREDITS
    else:
        neft_rtgs_flag = True
        company_name_flag = True
        amount_variation_check = True
        
    date_1 = salary_transactions[0]['date']
    date_2 = salary_transactions[1]['date']
    date_gap = abs((date_2 - date_1).days)
    days_gap = abs((date_2.day - date_1.day))
    if (neft_rtgs_flag or company_name_flag) and amount_variation_check and (
        (26 <= date_gap <= 36) or ((56 <= date_gap <= 66) and (days_gap<=3 or days_gap >=27))):
        salary_df = get_salary_processed_data(salary_df, month_list, standard_deviation_limit)
        if salary_df.empty:
            return dict()
        cnt_salary_months = salary_df['salary_month'].nunique()
        if (num_diff_month - cnt_salary_months <= 2) and (cnt_salary_months > 2):
            confidence_percentage += 10
        salary_transactions =salary_df.to_dict('records')
        return_data['salary_transactions'] = salary_transactions
        return_data['avg_monthly_salary_amount'] = possible_salaries_df['avg_monthly_salary_amount'].iloc[0]
        return_data['num_salary_months'] = salary_df['salary_month'].nunique()
        return_data['latest_salary_date'] = possible_salaries_df['latest_salary_date'].iloc[0]
        return_data['latest_salary_amount'] = possible_salaries_df['latest_salary_amount'].iloc[0]
        return_data['confidence_percentage'] = confidence_percentage
        
    return return_data

def separate_probable_salary_txn_grps(recurring_txns, transactions):
    hash_to_index_mapping = {}
    for i in range(len(transactions)):
        hash_to_index_mapping[transactions[i].get('hash')] = i

    recurring_credit_transactions = recurring_txns.get('recurring_credit_transactions')
    probable_salary_txn_grps = []
    allowed_transaction_channels = ['net_banking_transfer', 'salary', 'Other', 'other']

    for txn_grp in recurring_credit_transactions:
        each_grp_txns = []
        for grp_txn in txn_grp.get('transactions', []):
            if grp_txn.get('transaction_channel', None) in allowed_transaction_channels:
                transaction = deepcopy(transactions[hash_to_index_mapping[grp_txn.get('hash')]])
                transaction['clean_transaction_note'] = grp_txn['clean_transaction_note']
                transaction['employer_name'] = ""
                each_grp_txns.append(transaction)
        
        if each_grp_txns:
            probable_salary_txn_grps.append(each_grp_txns)
        
    return probable_salary_txn_grps

def keyword_contain_imps(df):
    contains_imps = df["transaction_note"].str.contains("IMPS", case=False, na=False).any()
    return contains_imps


def check_keyword_to_remove_flag(df):
    keywords = ["Claim", "Dividend", "loan", "EMI", "Disburse", "P2P", "Invoice", "Credit card", "overdraft"]
    pattern = "|".join(keywords)
    contains_keywords = df["transaction_channel"].str.contains(pattern, case=False, na=False)
    return contains_keywords.all()


def check_amount_deviation_5perc(df):
    latest_two_amounts = df["amount"].tail(2).values
    median_latest_two = pd.Series(latest_two_amounts).median()

    for _, row in df.iterrows():
        amount = row["amount"]
        amount_deviation = abs(amount - median_latest_two) / median_latest_two
        if amount_deviation > 0.05:
            return False

    return True


def check_keyword_flag(df):
    keywords = ["Pvt", "private", "consultancy", "services", "ltd", "Enterprises", "limited", "technology", "Tech", "Corporation", "Solutions", "LLP"]
    pattern = "|".join(keywords)
    contains_keywords = df["transaction_note"].str.contains(pattern, case=False, na=False)
    all_contains_keywords = contains_keywords.all()
    return all_contains_keywords


def check_amount_and_date_deviation(df):
    latest_two_amounts = df["amount"].tail(2).values
    median_latest_two = pd.Series(latest_two_amounts).median()
    earliest_date = df["date"].min()

    for _, row in df.iterrows():
        amount = row["amount"]
        transaction_date = row["date"]
        amount_deviation = abs(amount - median_latest_two) / median_latest_two
        if amount_deviation > 0.20:
            return False
        date_deviation = abs((transaction_date - earliest_date).days)
        if date_deviation > 5:
            return False
    return True


def new_confidence_calculator(possible_salaries, month_list, standard_deviation_limit, confidence_percentage, num_diff_month, has_debit_to_employer):
    return_data = dict()
    possible_salaries_df = pd.DataFrame(possible_salaries)
    print("possible_salaries_df ", possible_salaries_df)
    possible_salaries_df = possible_salaries_df.sort_values(by=["avg_monthly_salary_amount", "latest_salary_date"], ascending=[False, False])
    salary_transactions = possible_salaries_df["transaction_list"].iloc[0]
    salary_df = pd.DataFrame(salary_transactions)
    salary_df = get_salary_processed_data(salary_df, month_list, standard_deviation_limit)
    if salary_df.empty:
        return dict()

    end_of_month_flag = check_end_of_month_salary(salary_df)
    keyword_flag = check_keyword_flag(salary_df)
    amount_date_flag = check_amount_and_date_deviation(salary_df)
    perc5_flag = check_amount_deviation_5perc(salary_df)
    keyword_to_remove = check_keyword_to_remove_flag(salary_df)
    keyword_IMPS = keyword_contain_imps(salary_df)

    if salary_df.shape[0] >= 3:
        confidence_percentage += 10
    if end_of_month_flag:
        confidence_percentage += 10
    if keyword_flag:
        confidence_percentage += 10
    if amount_date_flag:
        confidence_percentage += 10
    if perc5_flag:
        confidence_percentage += 10
    if has_debit_to_employer:
        confidence_percentage -= 10
    if keyword_to_remove:
        confidence_percentage = min(confidence_percentage, 80)
    if keyword_IMPS:
        confidence_percentage = 50
    
    salary_transactions = salary_df.to_dict("records")
    return_data["salary_transactions"] = salary_transactions
    return_data["avg_monthly_salary_amount"] = possible_salaries_df["avg_monthly_salary_amount"].iloc[0]
    return_data["num_salary_months"] = salary_df["salary_month"].nunique()
    return_data["latest_salary_date"] = possible_salaries_df["latest_salary_date"].iloc[0]
    return_data["latest_salary_amount"] = possible_salaries_df["latest_salary_amount"].iloc[0]
    return_data["confidence_percentage"] = confidence_percentage

    return return_data