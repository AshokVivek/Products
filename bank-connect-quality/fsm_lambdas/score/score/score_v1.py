from score.score_woe_v1 import score_function
import warnings
import pandas as pd


warnings.simplefilter(action = "ignore", category = FutureWarning)
pd.options.mode.chained_assignment = None


def get_predictors_value(monthly_analysis ,key):

    # get the month_index which is present at the last of the predictor
    # prepare it for negative indexing
    index_month = (int(key.split("_")[-1][-1]) + 1) * -1
    predictor_name = "_".join(key.split("_")[:-1])
    value = None
    try:
        value = list(monthly_analysis.get(predictor_name).items())[index_month][1]
    except:
        value = None
    return value

def score_helper(payload):
    response = []
    try:
        for items in payload:
            predictors = items.get("predictors",{})
            monthly_analysis = items.get("monthly_analysis",{})
            account_id = items.get("account_id",None)

            columns=['avg_balance_without_loan_credit_multipleof5_3','avg_balance_without_loan_credit_multipleof5_4','avg_daily_closing_balance','bal_last_4','max_balance_0','min_balance_2','amt_business_credit_m3','amt_debit_m3','amt_debit_m4','amt_income_credit_m0','avg_bal_multipleof5_m4','avg_debit_transaction_size_m0','cnt_business_credit_m4','cnt_transactions_m6','max_eod_balance_m0','median_balance_m2','min_eod_balance_m2','min_eod_balance_m3','mode_balance_m2','turnover_excluding_loan_and_self_credit_m0']
            df = pd.DataFrame(columns=columns)
            row = []

            row.append(predictors.get('avg_balance_without_loan_credit_multipleof5_3'))
            row.append(predictors.get('avg_balance_without_loan_credit_multipleof5_4'))
            row.append(predictors.get('avg_daily_closing_balance'))
            row.append(predictors.get('bal_last_4'))
            row.append(predictors.get('max_balance_0'))
            row.append(predictors.get('min_balance_2'))
            
            row.append(get_predictors_value(monthly_analysis,'amt_business_credit_m3'))
            row.append(get_predictors_value(monthly_analysis,'amt_debit_m3'))
            row.append(get_predictors_value(monthly_analysis,'amt_debit_m4'))
            row.append(get_predictors_value(monthly_analysis,'amt_income_credit_m0'))
            row.append(get_predictors_value(monthly_analysis,'avg_bal_multipleof5_m4'))
            row.append(get_predictors_value(monthly_analysis,'avg_debit_transaction_size_m0'))
            row.append(get_predictors_value(monthly_analysis,'cnt_business_credit_m4'))
            row.append(get_predictors_value(monthly_analysis,'cnt_transactions_m6'))
            row.append(get_predictors_value(monthly_analysis,'max_eod_balance_m0'))
            row.append(get_predictors_value(monthly_analysis,'median_balance_m2'))
            row.append(get_predictors_value(monthly_analysis,'min_eod_balance_m2'))
            row.append(get_predictors_value(monthly_analysis,'min_eod_balance_m3'))
            row.append(get_predictors_value(monthly_analysis,'mode_balance_m2'))
            row.append(get_predictors_value(monthly_analysis,'turnover_excluding_loan_and_self_credit_m0'))
            
            df.loc[len(df)] = row
            
            param_row = dict()
            for i in range(len(columns)):
                param_row[columns[i]]=row[i]

            score = score_function(df)
            response.append({
                "account_id":account_id,
                "score":score,
                "score_version":"uis1.0",
                "params": param_row
            })
            
            print("Score for {} is {}".format(account_id, score))

        return {
            "status": 200,
            "scores":response
        }

    except Exception as e:
        print("Some Exception occured ", e)
        return {
            "status": 500,
            "scores": None,
            "message": "some error occured"
        }