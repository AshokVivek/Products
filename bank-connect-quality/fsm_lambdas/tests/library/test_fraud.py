from library.fraud import process_merged_pdf_transactions
import json
import pytest

@pytest.mark.parametrize(
    "event",[
        "single_inconsistency",
        "no_inconsistency",
        "missing_data",
        "empty_test",
        "very_high_inconsistency"
    ]
)

def test_fraud(event):
    
    data = json.load(open('tests/library/fraud_data.json', 'r'))
    data = data.get(event)
    transactions = data.get("transactions")
    expected_output = data.get('expected_output')
    statement_transactions, is_inconsistent, inconsistent_data, pages_updated = process_merged_pdf_transactions(transactions)
    
    test_data = {"transactions": statement_transactions, "is_inconsistent": is_inconsistent, "inconsistent_data": json.loads(json.dumps(inconsistent_data)), "pages_updated": list(pages_updated)}

    assert test_data == expected_output