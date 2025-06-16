import pytest
from moto import mock_aws
from python.api_subscriptions.get_account_transactions import get_account_transactions


@pytest.mark.parametrize(
    "event, expected_result",
    [
        # Account with transactions
        (
            {"entity_id": "672dd54b-d556-4315-8e12-0aa5337f147e", "account_id": "4cd9ebf5-be03-4172-941f-91b01aa3dfc0", "to_reject_account": False},
            [
                {
                    "transaction_type": "debit",
                    "transaction_note": "TO TRANSFER- UPI/DR/407715277144/Google I/UTIB/gpaybillpa/UPI-",
                    "chq_num": "TRANSFER TO 4897696162090",
                    "amount": 250.0,
                    "balance": 1925.81,
                    "date": "2024-03-17 00:00:00",
                    "hash": "171eaec4b56f4b600f07f599d91a55ab",
                    "category": "Utilities",
                },
                {
                    "transaction_type": "debit",
                    "transaction_note": "TO TRANSFER- UPI/DR/407715371864/EURON ETG/ICIC/euronetgpa/UPI-",
                    "chq_num": "TRANSFER TO 4897696162090",
                    "amount": 250.0,
                    "balance": 1675.81,
                    "date": "2024-03-17 00:00:00",
                    "hash": "fbdadcbaae23764bf514b7269b3dddfc",
                    "category": "Transfer to EURON ETG",
                },
                {
                    "transaction_type": "debit",
                    "transaction_note": "TO TRANSFER- UPI/DR/407729771414/Janta fa/YESB/paytmqrop3/UPI-",
                    "chq_num": "TRANSFER TO 4897696162090",
                    "amount": 65.0,
                    "balance": 1610.81,
                    "date": "2024-03-17 00:00:00",
                    "hash": "b88130c6f911901e670a5a011f2b91ab",
                    "category": "Transfer to JANTA FA",
                },
                {
                    "transaction_type": "debit",
                    "transaction_note": "TO TRANSFER- UPI/DR/407888119296/Shivsha n/YESB/paytmqrjgr/UPI-",
                    "chq_num": "TRANSFER TO 4897690162095",
                    "amount": 300.0,
                    "balance": 1310.81,
                    "date": "2024-03-18 00:00:00",
                    "hash": "f973d50a9f2fd6f1162f252ea5416c14",
                    "category": "Transfer to SHIVSHA N",
                },
                {
                    "transaction_type": "credit",
                    "transaction_note": "BY TRANSFER-INB IMPS407914005705/99999999 99/XX0000/Transferfu-",
                    "chq_num": "MAL00006612226 4 MAL00006612226 4",
                    "amount": 1.0,
                    "balance": 1311.81,
                    "date": "2024-03-19 00:00:00",
                    "hash": "053ebd2f8a01e6ca86f92bd831befbfd",
                    "category": "Others",
                },
            ],
        ),
        # Account with transactions where chq_num is none
        (
            {"entity_id": "b1456a4c-6398-4805-a015-63574376fa92", "account_id": "b1456a4c-6398-4805-a015-63574376fa92", "to_reject_account": False},
            [
                {
                    "transaction_type": "credit",
                    "transaction_note": "UPI-MR ANNAMALAI-9585782195@IBL-IDIB000B 059-451792742454-PAYMENT FROM PHONE",
                    "chq_num": None,
                    "amount": 100.0,
                    "balance": 298.5,
                    "date": "2024-05-30 00:00:00",
                    "hash": "25bc1992f9793f5e5d1fbd630a369d2a",
                    "category": "Transfer from MR ANNAMALAI",
                },
                {
                    "transaction_type": "debit",
                    "transaction_note": "UPI-ARAVINDHAN S-9600879603@YBL-SBIN000 0929-451781370931-PAYMENT FROM PHONE",
                    "chq_num": None,
                    "amount": 100.0,
                    "balance": 198.5,
                    "date": "2024-05-30 00:00:00",
                    "hash": "86c000b6c87e0767390a239feba51fa4",
                    "category": "Self Transfer",
                },
                {
                    "transaction_type": "debit",
                    "transaction_note": "UPI-KANNAN S-Q281413143@YBL-YESB0YBLUPI 451751357176-PAYMENT FROM PHONE",
                    "chq_num": None,
                    "amount": 115.0,
                    "balance": 83.5,
                    "date": "2024-05-30 00:00:00",
                    "hash": "564b095ceb640d0901b16ed04199b8c8",
                    "category": "Transfer to KANNAN S",
                },
                {
                    "transaction_type": "debit",
                    "transaction_note": "UPI-MURUGAN N-PAYTMQR2810050501011DEAAZ8 ZCXD3@PAYTM-YESB0PTMUPI-415242478103-PAY MENT FROM PHONE STATEMENT SUMMARY :-",
                    "chq_num": None,
                    "amount": 40.0,
                    "balance": 43.5,
                    "date": "2024-05-31 00:00:00",
                    "hash": "cff2b1132bb9e5c39f68ccfb09100c97",
                    "category": "Transfer to MURUGAN N",
                },
            ],
        ),
        (
            {
                "entity_id": "672dd54b-d556-4315-8e12-0aa5337f147e",
                "account_id": "4cd9ebf5-be03-4172-941f-91b01aa3dfc0",
                "to_reject_account": False,
                "enable_metadata": True,
            },
            [
                {
                    "transaction_type": "debit",
                    "transaction_note": "TO TRANSFER- UPI/DR/407715277144/Google I/UTIB/gpaybillpa/UPI-",
                    "chq_num": "TRANSFER TO 4897696162090",
                    "amount": 250.0,
                    "balance": 1925.81,
                    "date": "2024-03-17 00:00:00",
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
                    "date": "2024-03-17 00:00:00",
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
                    "date": "2024-03-17 00:00:00",
                    "hash": "b88130c6f911901e670a5a011f2b91ab",
                    "category": "Transfer to JANTA FA",
                    "metadata": {
                        "unclean_merchant": "JANTA FA",
                        "transaction_channel": "upi",
                        "description": "Transfer to JANTA FA",
                    },
                },
                {
                    "transaction_type": "debit",
                    "transaction_note": "TO TRANSFER- UPI/DR/407888119296/Shivsha n/YESB/paytmqrjgr/UPI-",
                    "chq_num": "TRANSFER TO 4897690162095",
                    "amount": 300.0,
                    "balance": 1310.81,
                    "date": "2024-03-18 00:00:00",
                    "hash": "f973d50a9f2fd6f1162f252ea5416c14",
                    "category": "Transfer to SHIVSHA N",
                    "metadata": {
                        "unclean_merchant": "SHIVSHA N",
                        "transaction_channel": "upi",
                        "description": "Transfer to SHIVSHA N",
                    },
                },
                {
                    "transaction_type": "credit",
                    "transaction_note": "BY TRANSFER-INB IMPS407914005705/99999999 99/XX0000/Transferfu-",
                    "chq_num": "MAL00006612226 4 MAL00006612226 4",
                    "amount": 1.0,
                    "balance": 1311.81,
                    "date": "2024-03-19 00:00:00",
                    "hash": "053ebd2f8a01e6ca86f92bd831befbfd",
                    "category": "Others",
                    "metadata": {
                        "unclean_merchant": "",
                        "transaction_channel": "net_banking_transfer",
                        "description": "",
                    },
                },
            ],
        ),
        # Account with no transactions
        ({"entity_id": "4e1bf704-8dad-42f1-8be1-af99cd19afe5", "account_id": "1145da38-a41b-4d8e-8bac-249ea471c4ed", "to_reject_account": False}, []),
    ],
)
@mock_aws
def test_get_account_transactions(event, expected_result, mocker, aws_dynamodb):
    assert get_account_transactions(event) == expected_result
