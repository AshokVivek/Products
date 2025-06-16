import pytest
from moto import mock_aws
from python.api_subscriptions.get_account_fraud import get_account_fraud


@pytest.mark.parametrize(
    "event, expected_result",
    [
        # Case 11: Account with disparity
        (
            {"entity_id": "6f0586b1-e834-4d43-904e-f0615376b1ff", "account_id": "fd54e44c-9076-4ac7-abc9-5ca177fc1e2a", "to_reject_account": False},
            [
                {
                    "statement_id": "b4663e5a-a795-418c-8e1d-f2d15a015251",
                    "fraud_type": "author_fraud",
                    "transaction_hash": None,
                    "fraud_category": "metadata",
                },
                {
                    "statement_id": "d88e3b07-a881-4681-97f5-9b8ac6adac73",
                    "fraud_type": "inconsistent_transaction",
                    "transaction_hash": "184a599753d81b210dc2aa8a83228db0",
                    "fraud_category": "accounting",
                },
                {
                    "statement_id": "b4663e5a-a795-418c-8e1d-f2d15a015251",
                    "fraud_type": "inconsistent_transaction",
                    "transaction_hash": "184a599753d81b210dc2aa8a83228db0",
                    "fraud_category": "accounting",
                },
                {
                    "statement_id": None,
                    "fraud_type": "inconsistent_transaction",
                    "transaction_hash": "184a599753d81b210dc2aa8a83228db0",
                    "fraud_category": "accounting",
                },
                {
                    "statement_id": None,
                    "fraud_type": "min_rtgs_amount",
                    "transaction_hash": "e0ab7842fc31fd7e84338ebc1ebe037c",
                    "fraud_category": "transaction",
                },
            ],
        ),
    ],
)
@mock_aws
def test_get_account_details(event, expected_result, mocker, aws_dynamodb):
    assert get_account_fraud(event) == expected_result
