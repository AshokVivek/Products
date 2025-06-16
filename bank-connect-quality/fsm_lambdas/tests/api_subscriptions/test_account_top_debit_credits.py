import pytest
from moto import mock_aws
from python.api_subscriptions.get_account_top_debit_credits import get_account_top_debit_credits


@pytest.mark.parametrize(
    "event, expected_result",
    [
        # Account with top debit credits
        (
            {"entity_id": "672dd54b-d556-4315-8e12-0aa5337f147e", "account_id": "4cd9ebf5-be03-4172-941f-91b01aa3dfc0", "to_reject_account": True},
            [
                {
                    "type": "top_5_debit",
                    "data": [
                        {
                            "month": "Mar-24",
                            "data": [
                                {
                                    "transaction_type": "debit",
                                    "transaction_note": "TO TRANSFER- UPI/DR/407888119296/Shivsha n/YESB/paytmqrjgr/UPI-",
                                    "chq_num": "TRANSFER TO 4897690162095",
                                    "amount": 300.0,
                                    "balance": 1310.81,
                                    "date": "18-Mar-24",
                                    "hash": "f973d50a9f2fd6f1162f252ea5416c14",
                                    "category": "Transfer to SHIVSHA N",
                                },
                                {
                                    "transaction_type": "debit",
                                    "transaction_note": "TO TRANSFER- UPI/DR/407715277144/Google I/UTIB/gpaybillpa/UPI-",
                                    "chq_num": "TRANSFER TO 4897696162090",
                                    "amount": 250.0,
                                    "balance": 1925.81,
                                    "date": "17-Mar-24",
                                    "hash": "171eaec4b56f4b600f07f599d91a55ab",
                                    "category": "Utilities",
                                },
                                {
                                    "transaction_type": "debit",
                                    "transaction_note": "TO TRANSFER- UPI/DR/407715371864/EURON ETG/ICIC/euronetgpa/UPI-",
                                    "chq_num": "TRANSFER TO 4897696162090",
                                    "amount": 250.0,
                                    "balance": 1675.81,
                                    "date": "17-Mar-24",
                                    "hash": "fbdadcbaae23764bf514b7269b3dddfc",
                                    "category": "Transfer to EURON ETG",
                                },
                                {
                                    "transaction_type": "debit",
                                    "transaction_note": "TO TRANSFER- UPI/DR/407729771414/Janta fa/YESB/paytmqrop3/UPI-",
                                    "chq_num": "TRANSFER TO 4897696162090",
                                    "amount": 65.0,
                                    "balance": 1610.81,
                                    "date": "17-Mar-24",
                                    "hash": "b88130c6f911901e670a5a011f2b91ab",
                                    "category": "Transfer to JANTA FA",
                                },
                            ],
                        }
                    ],
                },
                {
                    "type": "top_5_credit",
                    "data": [
                        {
                            "month": "Mar-24",
                            "data": [
                                {
                                    "transaction_type": "credit",
                                    "transaction_note": "BY TRANSFER-INB IMPS407914005705/99999999 99/XX0000/Transferfu-",
                                    "chq_num": "MAL00006612226 4 MAL00006612226 4",
                                    "amount": 1.0,
                                    "balance": 1311.81,
                                    "date": "19-Mar-24",
                                    "hash": "053ebd2f8a01e6ca86f92bd831befbfd",
                                    "category": "Others",
                                }
                            ],
                        }
                    ],
                },
            ],
        ),
        (
            {
                "entity_id": "672dd54b-d556-4315-8e12-0aa5337f147e",
                "account_id": "4cd9ebf5-be03-4172-941f-91b01aa3dfc0",
                "to_reject_account": True,
                "enable_metadata": True,
            },
            [
                {
                    "type": "top_5_debit",
                    "data": [
                        {
                            "month": "Mar-24",
                            "data": [
                                {
                                    "transaction_type": "debit",
                                    "transaction_note": "TO TRANSFER- UPI/DR/407888119296/Shivsha n/YESB/paytmqrjgr/UPI-",
                                    "chq_num": "TRANSFER TO 4897690162095",
                                    "amount": 300.0,
                                    "balance": 1310.81,
                                    "date": "18-Mar-24",
                                    "hash": "f973d50a9f2fd6f1162f252ea5416c14",
                                    "category": "Transfer to SHIVSHA N",
                                    "metadata": {
                                        "unclean_merchant": "SHIVSHA N",
                                        "transaction_channel": "upi",
                                        "description": "Transfer to SHIVSHA N",
                                    },
                                },
                                {
                                    "transaction_type": "debit",
                                    "transaction_note": "TO TRANSFER- UPI/DR/407715277144/Google I/UTIB/gpaybillpa/UPI-",
                                    "chq_num": "TRANSFER TO 4897696162090",
                                    "amount": 250.0,
                                    "balance": 1925.81,
                                    "date": "17-Mar-24",
                                    "hash": "171eaec4b56f4b600f07f599d91a55ab",
                                    "category": "Utilities",
                                    "metadata": {
                                        "unclean_merchant": "GOOGLE I",
                                        "transaction_channel": "upi",
                                        "description": "Transfer to GOOGLE I",
                                    },
                                },
                                {
                                    "transaction_type": "debit",
                                    "transaction_note": "TO TRANSFER- UPI/DR/407715371864/EURON ETG/ICIC/euronetgpa/UPI-",
                                    "chq_num": "TRANSFER TO 4897696162090",
                                    "amount": 250.0,
                                    "balance": 1675.81,
                                    "date": "17-Mar-24",
                                    "hash": "fbdadcbaae23764bf514b7269b3dddfc",
                                    "category": "Transfer to EURON ETG",
                                    "metadata": {
                                        "unclean_merchant": "EURON ETG",
                                        "transaction_channel": "upi",
                                        "description": "Transfer to EURON ETG",
                                    },
                                },
                                {
                                    "transaction_type": "debit",
                                    "transaction_note": "TO TRANSFER- UPI/DR/407729771414/Janta fa/YESB/paytmqrop3/UPI-",
                                    "chq_num": "TRANSFER TO 4897696162090",
                                    "amount": 65.0,
                                    "balance": 1610.81,
                                    "date": "17-Mar-24",
                                    "hash": "b88130c6f911901e670a5a011f2b91ab",
                                    "category": "Transfer to JANTA FA",
                                    "metadata": {
                                        "unclean_merchant": "JANTA FA",
                                        "transaction_channel": "upi",
                                        "description": "Transfer to JANTA FA",
                                    },
                                },
                            ],
                        }
                    ],
                },
                {
                    "type": "top_5_credit",
                    "data": [
                        {
                            "month": "Mar-24",
                            "data": [
                                {
                                    "transaction_type": "credit",
                                    "transaction_note": "BY TRANSFER-INB IMPS407914005705/99999999 99/XX0000/Transferfu-",
                                    "chq_num": "MAL00006612226 4 MAL00006612226 4",
                                    "amount": 1.0,
                                    "balance": 1311.81,
                                    "date": "19-Mar-24",
                                    "hash": "053ebd2f8a01e6ca86f92bd831befbfd",
                                    "category": "Others",
                                    "metadata": {
                                        "unclean_merchant": "",
                                        "transaction_channel": "net_banking_transfer",
                                        "description": "",
                                    },
                                }
                            ],
                        }
                    ],
                },
            ],
        ),
        # Account with no top debit credits
        (
            {"entity_id": "4e1bf704-8dad-42f1-8be1-af99cd19afe5", "account_id": "1145da38-a41b-4d8e-8bac-249ea471c4ed", "to_reject_account": False},
            [{"type": "top_5_debit", "data": []}, {"type": "top_5_credit", "data": []}],
        ),
    ],
)
@mock_aws
def test_get_account_top_debit_credits(event, expected_result, mocker, aws_dynamodb):
    assert get_account_top_debit_credits(event) == expected_result
