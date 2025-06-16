import pytest
from moto import mock_aws
from python.api_subscriptions.get_account_details import get_account_details


@pytest.mark.parametrize(
    "event, expected_result",
    [
        # Case 1
        (
            {
                "entity_id": "6935038b-0c6e-4696-92b5-4325832654",
                "account_id": "fd4c00bc-45da-4b37-8a7d-843256324859",
                "to_reject_account": False,
            },
            {
                "account_number": "XXXXXXXXXXX8105",
                "bank": "axis",
                "country_code": "IN",
                "currency_code": "INR",
                "metadata_analysis": {"name_matches": []},
                "missing_data": [],
                "statements": ["324a9f63-c644-4c77-b1a7-68ac8fdf9b03"],
                "months": ["2023-08", "2023-09", "2023-10", "2023-11"],
                "uploaded_months": [],
                "name": "MOHAMMAD JASIM",
                "address": "GOKUL NAGAR ATAL VIHAR MATHPURENA,RAIPUR BEHIND GARDEN, ,RAIPUR,CHHATTISGARH,INDIA,492013",
                "account_category": "SAVINGS",
                "account_opening_date": None,
                "credit_limit": None,
                "od_limit": None,
                "ifsc": "UTIB0002091",
                "micr": "",
                "salary_confidence": None,
                "dob": "",
                "email": "",
                "pan_number": "",
                "phone_number": "",
                "account_status": "",
                "holder_type": "",
                "account_date_range": {
                    "from_date": None,
                    "to_date": None
                },
                "transaction_date_range": {
                    "from_date": None,
                    "to_date": None
                }
            },
        ),
        # Case 2
        (
            {
                "entity_id": "c925471a-7910-416d-8c16-8eca0db98b21",
                "account_id": "13076613-2983-4f2d-9f12-8333484a979f",
                "to_reject_account": True,
            },
            {
                "account_number": "00000041965448665",
                "bank": "sbi",
                "country_code": "IN",
                "currency_code": "INR",
                "metadata_analysis": {"name_matches": []},
                "missing_data": [
                    {"from_date": "2024-01-03", "to_date": "2024-01-31"},
                    {"from_date": "2023-10-30", "to_date": "2023-11-09"},
                ],
                "statements": [
                    "4be69fee-b290-444e-8ed3-56b41d13b7fa",
                    "0d623233-1ed1-4d40-a182-3cd5b388ddcb",
                ],
                "months": ["2023-11", "2023-12", "2024-01"],
                "uploaded_months": ['2023-11', '2023-12', '2024-01'],
                "name": "MR. MD MAHMUD ALAM",
                "address": "C/O: Md Mustafa Alam, #328 24th Ward, Ka wadi Street, Near Parvatamma Temple Bellary-583102 Bellary",
                "account_category": "individual",
                "account_opening_date": None,
                "credit_limit": 0,
                "od_limit": 0,
                "ifsc": "SBIN0040722",
                "micr": "583002127",
                "salary_confidence": None,
                "dob": "",
                "email": "",
                "pan_number": "",
                "phone_number": "",
                "account_status": "",
                "holder_type": "",
                "account_date_range": {
                    "from_date": None,
                    "to_date": None
                },
                "transaction_date_range": {
                    "from_date": None,
                    "to_date": None
                }
            },
        ),
        # Case 4
        (
            {
                "entity_id": "7cf941af-191b-4c79-adcb-bba5f29e3510",
                "account_id": "423b55fb-b2ef-4134-a93c-c8147f950167",
                "to_reject_account": True,
            },
            {
                "account_number": "7147642094",
                "bank": "kotak",
                "country_code": "IN",
                "currency_code": "INR",
                "metadata_analysis": {"name_matches": []},
                "missing_data": [],
                "statements": ["fbe05364-d287-45a8-989c-db7b4e2dee5a"],
                "months": [
                    "2023-06",
                    "2023-07",
                    "2023-08",
                    "2023-09",
                    "2023-10",
                    "2023-11",
                ],
                "uploaded_months": [],
                "name": "Sanjay",
                "address": "H N0-B-99 STREET N0-4 RAMA GAR DEN KARAWAL NAGAR NORTH EAST New Delhi - 110094",
                "account_category": "individual",
                "account_opening_date": None,
                "credit_limit": None,
                "od_limit": None,
                "ifsc": "",
                "micr": "",
                "salary_confidence": None,
                "dob": "",
                "email": "",
                "pan_number": "",
                "phone_number": "",
                "account_status": "",
                "holder_type": "",
                "account_date_range": {
                    "from_date": None,
                    "to_date": None
                },
                "transaction_date_range": {
                    "from_date": None,
                    "to_date": None
                }
            },
        ),
        # Case 5
        (
            {
                "entity_id": "11e618c5-6869-487d-b999-74ffc0b481e1",
                "account_id": "ec9fde7b-eb06-4958-8e9b-ad7ca2c51397",
                "to_reject_account": True,
            },
            {
                "account_number": "157764016923",
                "bank": "indusind",
                "country_code": "IN",
                "currency_code": "INR",
                "metadata_analysis": {"name_matches": []},
                "missing_data": [],
                "statements": [
                    "f1031160-ba89-4705-b103-fb3585eeadb4",
                    "d36556ad-e315-4789-85d0-3ed87c205165",
                    "61a7acd6-be6f-4ed5-aefa-eb881ded88b4",
                ],
                "months": ["2024-05"],
                "uploaded_months": ["2024-05"],
                "name": "SHAHANAWAJ ALAM",
                "address": "BASTHA, BASTHA, MAINATAND, BASTHA,WEST CHAMPARAN, WARD NO 12, WEST CHAMPARAN,BIHAR, INDIA-845306",
                "account_category": "individual",
                "account_opening_date": None,
                "credit_limit": 0,
                "od_limit": 0,
                "ifsc": None,
                "micr": None,
                "salary_confidence": 70,
                "dob": "",
                "email": "",
                "pan_number": "",
                "phone_number": "",
                "account_status": "",
                "holder_type": "",
                "account_date_range": {
                    "from_date": None,
                    "to_date": None
                },
                "transaction_date_range": {
                    "from_date": None,
                    "to_date": None
                }
            },
        ),
        # Case 7: Country and currency code not present
        (
            {
                "entity_id": "bc870d10-b904-465c-8e11-ecb9f1825707",
                "account_id": "25e32c76-d2fc-4a6c-87df-d55e4a1b8455",
                "to_reject_account": True,
            },
            {
                "account_number": "011310100291672",
                "bank": "ubi",
                "country_code": "IN",
                "currency_code": "INR",
                "metadata_analysis": {"name_matches": []},
                "missing_data": [],
                "statements": [
                    "e50b64ce-c511-4c43-ba84-ab40f24476e5",
                    "f8de024e-6151-4342-9ef9-118505a62eb4",
                    "ff0082c0-34eb-4645-b2a0-d472ebbcc2bf",
                    "4419f0a7-a4da-4b5e-a15b-d87678467f14",
                ],
                "months": ["2024-03"],
                "uploaded_months": ["2024-03"],
                "name": None,
                "address": None,
                "account_category": "individual",
                "account_opening_date": None,
                "credit_limit": 0,
                "od_limit": 0,
                "ifsc": "UBIN0801135",
                "micr": None,
                "salary_confidence": None,
                "dob": "",
                "email": "",
                "pan_number": "",
                "phone_number": "",
                "account_status": "",
                "holder_type": "",
                "account_date_range": {
                    "from_date": None,
                    "to_date": None
                },
                "transaction_date_range": {
                    "from_date": None,
                    "to_date": None
                }
            },
        ),
        # Case 10: missing_data not present
        (
            {
                "entity_id": "bdc363de-fd2e-4ffd-a53f-7a8db81ff612",
                "account_id": "30b30d95-305f-4322-a378-28e10cbeb4f1",
                "to_reject_account": True,
            },
            {
                "account_number": "00000034249671065",
                "bank": "sbi",
                "country_code": "IN",
                "currency_code": "INR",
                "metadata_analysis": {"name_matches": []},
                "missing_data": [],
                "statements": [
                    "eaf5fe32-fd7b-4d38-90e5-038d4e2669af",
                    "e82a03dc-7c30-4ceb-87d1-7c2b240c4f4a",
                    "b820a3de-1922-40f0-a515-4232330af44f",
                ],
                "months": ["2023-11", "2023-12", "2024-01"],
                'uploaded_months': ['2023-11', '2023-12', '2024-01', '2024-02', '2024-03', '2024-04'],
                "name": None,
                "address": None,
                "account_category": "individual",
                "account_opening_date": None,
                "credit_limit": 0,
                "od_limit": 0,
                "ifsc": "SBIN0008307",
                "micr": "281002050",
                "salary_confidence": None,
                "dob": "",
                "email": "",
                "pan_number": "",
                "phone_number": "",
                "account_status": "",
                "holder_type": "",
                "account_date_range": {
                    "from_date": None,
                    "to_date": None
                },
                "transaction_date_range": {
                    "from_date": None,
                    "to_date": None
                }
            },
        ),
    ],
)
@mock_aws
def test_get_account_details(event, expected_result, mocker, aws_dynamodb):
    assert get_account_details(event) == expected_result


@pytest.mark.parametrize(
    "event, expected_exception",
    [
        # Case 6: Statements not present
        (
            {
                "entity_id": "1b2e302c-a058-4fad-af42-20d4c786e1c3",
                "account_id": "e81101e6-2180-4e74-8d98-f5038e64e0b0",
                "to_reject_account": True,
            },
            IndexError,
        ),
        # Case 3: account_number is None
        (
            {
                "entity_id": "c501cf3f-3dfc-4807-9e78-94d1e763e0b9",
                "account_id": "155fae13-94ed-462e-b7a8-b6666a1ccac9",
                "to_reject_account": True,
            },
            ValueError,
        ),
        # Case 11: entity_id not present
        (
            {
                "entity_id": "not_found_entity",
                "account_id": "155fae13-94ed-462e-b7a8-b6666a1ccac9",
                "to_reject_account": True,
            },
            ValueError,
        ),
        # Case 12: account_id not present
        (
            {
                "entity_id": "bc870d10-b904-465c-8e11-ecb9f1825707",
                "account_id": "not_found_account",
                "to_reject_account": True,
            },
            Exception,
        ),
        # Case 8: date_range not present in identity
        (
            {
                "entity_id": "e78f9776-2553-4ac5-90a7-04015d8a2046",
                "account_id": "24f4f3d6-4d3d-4f79-af70-8f68216c378f",
                "to_reject_account": True,
            },
            Exception,
        ),
        # Case 9: account_status is failed
        (
            {
                "entity_id": "bdc363de-fd2e-4ffd-a53f-7a8db8aff612",
                "account_id": "30b30d95-305f-4322-a378-28ed0cbeb4f1",
                "to_reject_account": True,
            },
            Exception,
        ),
    ],
)
@mock_aws
def test_get_account_details_error(event, expected_exception, mocker, aws_dynamodb):
    with pytest.raises(expected_exception):
        get_account_details(event)
