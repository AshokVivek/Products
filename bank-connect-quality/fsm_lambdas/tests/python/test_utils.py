import pytest
import os
import sys
from datetime import datetime
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from python.utils import (
    get_date_of_format,
    get_datetime,
    compare_account_numbers
)


@pytest.mark.parametrize(
    "date_str, format, expected_result",
    [
        ("2024-08-31", "%Y-%m-%d", "2024-08-31"),
        ("2024/08/31", "%Y-%m-%d", "2024-08-31"),
        ("2024-08-31", "%Y-%m-%d", "2024-08-31"),
        ("2024-08-31", "%Y-%d-%m", "2024-31-08"),
        ("20240831", "%Y-%m-%d", "2024-08-31"),
        ("2024-09-31", "%Y-%m-%d", None),
        ("2024-08-33", "%Y-%m-%d", None),
        ("2024-18-31", "%Y-%m-%d", None),
        ("2024-08-31", "", ""),
        ("", "%Y-%m-%d", None),
        ("", "", None),
        (None, "", None),
        ("", None, None),
        (None, None, None)
    ],
)
def test_get_date_of_format(date_str, format, expected_result):
    assert get_date_of_format(date_str, format) == expected_result


@pytest.mark.parametrize(
    "date_str, formats, expected_result",
    [
        ("2024-09-12 12:33:22.34", ["%Y-%m-%d %H:%M:%S.%f"], datetime(2024, 9, 12, 12, 33, 22, 340000)),
        ("2024-09-12 12:33:22.34", None, datetime(2024, 9, 12, 12, 33, 22, 340000)),
        ("2024-09-12 12:33:22.34", [], datetime(2024, 9, 12, 12, 33, 22, 340000)),
        ("2024-09-12 12:33:22.34", ["%Y-%m-%d %H:%M:%S"], None),
        ("2024-09-12 12:33:22", ["%Y-%m-%d %H:%M:%S"], datetime(2024, 9, 12, 12, 33, 22)),
        ("2024-09-12 12:33:22", None, datetime(2024, 9, 12, 12, 33, 22)),
        ("2024-09-12 12:33:22", [], datetime(2024, 9, 12, 12, 33, 22)),
        ("2024-09-12 12:33:22", ["%Y-%m-%d %H:%M:%S.%f"], None),
        ("2024-13-09 12:33:22", [], None),
        ("2024/09/12 12:33:22", [], None),
        ("2024-09-12", [], None),
        (1.2, None, None),
        (20240831, None, None),
    ]
)
def test_get_datetime(date_str, formats, expected_result):
    assert get_datetime(date_str, formats) == expected_result


@pytest.mark.parametrize(
    "account_number, curr_account_number, expected_result",
    [
        ("123456789", "123456789", True),  # Exact match
        ("123-456-789", "123456789", True),  # Match ignoring hyphens
        ("123 456 789", "123456789", True),  # Match ignoring spaces
        ("123456789", "000123456789", True),  # Leading zeros ignored
        ("123XXX789", "123456789", True),  # 'X' acts as a wildcard
        ("123XXX789", "123YYY789", True),  # Last three match, letters ignored
        ("123XXX780", "123456789", False),  # Last three don't match
        ("1234-567X", "1234-5678", True),  # 'X' wildcard
        ("AB1234567", "XY1234567", True),  # Alphabets, last three should match
        ("123456", "654321", False),  # Last three do not match
        ("123456", "1234567", False),  # Length mismatch without valid match
        ("123456789", "987654321", False),  # Completely different numbers
        ("5-0XX5XX-X38", "5001505238", True),
        ("5-3XX6XX-X16", "5376606816", True),
        ("00056290", "002010100056290", True),
    ]
)
def test_compare_account_numbers(account_number, curr_account_number, expected_result):
    assert compare_account_numbers(account_number, curr_account_number) == expected_result