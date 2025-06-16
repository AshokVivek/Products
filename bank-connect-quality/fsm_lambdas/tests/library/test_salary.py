import json
import pytest
import pandas as pd
from library.salary import get_salary_transactions

EMPLOYER_NAMES_JSON = {
    "1eebfabc-44c4-42c3-9abd-fa8434f55ea0": ["Minar alloys and forgings pvt Ltd"],
    "c456d0ad-0db5-4e8a-b835-23bc73980f53": ["sunshine Sri saradha secondary school "],
    "c6b17ca6-110d-47b0-9926-386b61d9983e": ["Tata Consultancy Services limited"],
    "f53f28ef-f368-4585-8e81-a28f127442a7": ["government of Andhra Pradesh "],
    "ce1f3f02-93c0-4bf1-aa6e-9abb25669fca": ["Shanti Gopal Hospital"]
}


def get_salary_output(transactions=[], account_id=""):
    comparison_dict = {}
    employer_names = EMPLOYER_NAMES_JSON.get(account_id, [])
    salary_dict = get_salary_transactions(transactions, employer_names, recurring_salary_flag=True, salary_mode='HARD')
    salary_transactions = salary_dict.get("salary_transactions", [])
    if salary_transactions:
        salary_df = pd.DataFrame(salary_transactions)
        comparison_dict['no_of_transactions'] = len(salary_transactions)
        comparison_dict['avg_monthly_salary_amount'] = round(salary_dict.get("avg_monthly_salary_amount", 0), 0)
        comparison_dict['num_salary_months'] = salary_dict.get("num_salary_months", 0)
        comparison_dict['latest_salary_amount'] = round(salary_dict.get("latest_salary_amount", 0), 0)
        comparison_dict['confidence_percentage'] = salary_dict.get("confidence_percentage", None)
        comparison_dict['max_salary'] = round(salary_df['amount'].max(), 0)
        comparison_dict['min_salary'] = round(salary_df['amount'].min(), 0)
        comparison_dict['calculation_method'] = salary_transactions[0].get("calculation_method", None)
        
    return comparison_dict


@pytest.mark.parametrize(
    "event, expected_result",
    [
        ## Case1: Salary method == "recurring"
        # (
        #     {"account_id": "e206cdfe-ae4b-4393-abd0-5709f18ebb35"}, 
        #     {
        #         "no_of_transactions": 3,
        #         "avg_monthly_salary_amount": 12540.0,
        #         "num_salary_months": 3,
        #         "latest_salary_amount": 12211.0,
        #         "confidence_percentage": 90,
        #         "max_salary": 13204.0,
        #         "min_salary": 12205.0,
        #         "calculation_method": "recurring"
        #     }
        # ),
        # (
        #     {"account_id": "5ad09138-f8ff-4e66-a664-cf616516fa1a"}, 
        #     {
        #         "no_of_transactions": 3,
        #         "avg_monthly_salary_amount": 97454.0,
        #         "num_salary_months": 3,
        #         "latest_salary_amount": 80400.0,
        #         "confidence_percentage": 70,
        #         "max_salary": 93250.0,
        #         "min_salary": 80400.0,
        #         "calculation_method": "recurring"
        #     }
        # ),
        # (
        #     {"account_id": "824bae4c-7192-443d-bf18-50a23fd2ecbc"}, 
        #     {
        #         "no_of_transactions": 2,
        #         "avg_monthly_salary_amount": 126522.0,
        #         "num_salary_months": 2,
        #         "latest_salary_amount": 124984.0,
        #         "confidence_percentage": 70,
        #         "max_salary": 128060.0,
        #         "min_salary": 124984.0,
        #         "calculation_method": "recurring"
        #     }
        # ),
        # (
        #     {"account_id": "52e19df3-65c7-49fe-a0bc-fd107b8a6004"},
        #     {
        #         "no_of_transactions": 5,
        #         "avg_monthly_salary_amount": 19800.0,
        #         "num_salary_months": 5,
        #         "latest_salary_amount": 20017.0,
        #         "confidence_percentage": 80,
        #         "max_salary": 24416.0,
        #         "min_salary": 17036.0,
        #         "calculation_method": "recurring"
        #     }
        # ),
        # (
        #     {"account_id": "4467547c-04fc-48ab-b7bb-be825eb5fc15"},
        #     {
        #         "no_of_transactions": 4,
        #         "avg_monthly_salary_amount": 10858.0,
        #         "num_salary_months": 4,
        #         "latest_salary_amount": 11811.0,
        #         "confidence_percentage": 90,
        #         "max_salary": 11811.0,
        #         "min_salary": 9474.0,
        #         "calculation_method": "recurring"
        #     }
        # ),
        # (
        #     {"account_id": "532a176a-f7c3-4427-abb4-9da32edf67e8"},
        #     {
        #         "no_of_transactions": 5,
        #         "avg_monthly_salary_amount": 25000.0,
        #         "num_salary_months": 5,
        #         "latest_salary_amount": 25000.0,
        #         "confidence_percentage": 60,
        #         "max_salary": 25000.0,
        #         "min_salary": 25000.0,
        #         "calculation_method": "recurring"
        #     }
        # ),
        # (
        #     {"account_id": "495e5826-6b98-4753-86c5-3d1d91bf04ac"},
        #     {
        #         "no_of_transactions": 12,
        #         "avg_monthly_salary_amount": 23160.0,
        #         "num_salary_months": 12,
        #         "latest_salary_amount": 24212.0,
        #         "confidence_percentage": 90,
        #         "max_salary": 24212.0,
        #         "min_salary": 22528.0,
        #         "calculation_method": "recurring"
        #     }
        # ),
        
        ## Case2: Salary method == "keyword"
        (
            {"account_id": "0e1eec74-d3f2-44b4-b0fa-db562f954a0b"}, 
            {
                "no_of_transactions": 5,
                "avg_monthly_salary_amount": 41196.0,
                "num_salary_months": 5,
                "latest_salary_amount": 41200.0,
                "confidence_percentage": 100,
                "max_salary": 41200.0,
                "min_salary": 41180.0,
                "calculation_method": "keyword"
            }
        ),
        (
            {"account_id": "0173b73a-9a3b-4998-a0ff-f5f09a845dab"}, 
            {
                "no_of_transactions": 6,
                "avg_monthly_salary_amount": 17047.0,
                "num_salary_months": 6,
                "latest_salary_amount": 18881.0,
                "confidence_percentage": 100,
                "max_salary": 25344.0,
                "min_salary": 9268.0,
                "calculation_method": "keyword"
            }
        ),
        (
            {"account_id": "1132b2ad-9e7c-4523-8774-96aa0cc67f02"}, 
            {
                "no_of_transactions": 3,
                "avg_monthly_salary_amount": 39260.0,
                "num_salary_months": 3,
                "latest_salary_amount": 36775.0,
                "confidence_percentage": 100,
                "max_salary": 41693.0,
                "min_salary": 36775.0,
                "calculation_method": "keyword"
            }
        ),
        (
            {"account_id": "79493bd5-16a1-4e04-b8ad-ea2b3bc5b143"}, 
            {
                "no_of_transactions": 3,
                "avg_monthly_salary_amount": 109192.0,
                "num_salary_months": 3,
                "latest_salary_amount": 129743.0,
                "confidence_percentage": 100,
                "max_salary": 133740.0,
                "min_salary": 64093.0,
                "calculation_method": "keyword"
            }
        ),
        (
            {"account_id": "c9aa42d2-9f15-499d-8fa0-f4b2b5b182a0"}, 
            {
                "no_of_transactions": 6,
                "avg_monthly_salary_amount": 57044.0,
                "num_salary_months": 6,
                "latest_salary_amount": 46600.0,
                "confidence_percentage": 100,
                "max_salary": 63770.0,
                "min_salary": 46600.0,
                "calculation_method": "keyword"
            }
        ),
        
        ## Case3: Salary method == "employer_name"
        (
            {"account_id": "1eebfabc-44c4-42c3-9abd-fa8434f55ea0"}, 
            {
                "no_of_transactions": 14,
                "avg_monthly_salary_amount": 15552.0,
                "num_salary_months": 14,
                "latest_salary_amount": 15552.0,
                "confidence_percentage": 100,
                "max_salary": 15552.0,
                "min_salary": 15552.0,
                "calculation_method": "employer_name"
            }
        ),
        (
            ### Keyword & Employer Name both are present
            {"account_id": "c456d0ad-0db5-4e8a-b835-23bc73980f53"}, 
            {
                "no_of_transactions": 5,
                "avg_monthly_salary_amount": 29645.0,
                "num_salary_months": 5,
                "latest_salary_amount": 29750.0,
                "confidence_percentage": 100,
                "max_salary": 29750.0,
                "min_salary": 29224.0,
                "calculation_method": "keyword"
            }
        ),
        (
            {"account_id": "c6b17ca6-110d-47b0-9926-386b61d9983e"}, 
            {
                "no_of_transactions": 6,
                "avg_monthly_salary_amount": 25302.0,
                "num_salary_months": 6,
                "latest_salary_amount": 25179.0,
                "confidence_percentage": 100,
                "max_salary": 26767.0,
                "min_salary": 24965.0,
                "calculation_method": "employer_name"
            }
        ),
        (
            {"account_id": "f53f28ef-f368-4585-8e81-a28f127442a7"}, 
            {
                "no_of_transactions": 6,
                "avg_monthly_salary_amount": 28294.0,
                "num_salary_months": 6,
                "latest_salary_amount": 28788.0,
                "confidence_percentage": 100,
                "max_salary": 28788.0,
                "min_salary": 28032.0,
                "calculation_method": "employer_name"
            }
        ),
        (
            {"account_id": "ce1f3f02-93c0-4bf1-aa6e-9abb25669fca"}, 
            {
                "no_of_transactions": 5,
                "avg_monthly_salary_amount": 36241.0,
                "num_salary_months": 5,
                "latest_salary_amount": 32136.0,
                "confidence_percentage": 100,
                "max_salary": 39769.0,
                "min_salary": 32136.0,
                "calculation_method": "employer_name"
            }
        ),
        
        ## Case4: Non Salaried Accounts
        (
            {"account_id": "6b81caaf-380f-45b8-bf67-b430877bce79"}, 
            {}
        ),
        (
            {"account_id": "86b99a4e-d156-45d4-9284-792775342ecf"}, 
            {}
        ),
        (
            {"account_id": "cec025aa-53c2-48f8-8e11-4f6470573adc"}, 
            {}
        ),
        (
            {"account_id": "c1fbd537-066b-493b-a0c7-43388848e3b7"}, 
            {}
        ),
        (
            {"account_id": "eba8f536-f20d-44e8-ba76-5279247f7dff"}, 
            {}
        )
    ]
)

def test_salary(event, expected_result):
    account_id = event.get("account_id", "")
    transactions = json.load(open(f'tests/library/transactions_data/transactions_{account_id}.json', 'r'))['transactions']
    salary_dict = get_salary_output(transactions, account_id)
    
    # this is a test and assert is supposed to be present here, does not qualify as a vulnerability
    assert salary_dict==expected_result # nosec