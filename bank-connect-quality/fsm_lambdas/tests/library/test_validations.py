import pytest
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from library.validations import date_order




@pytest.mark.parametrize(
    "date_list, expected_result",
    [
        ([datetime(2023, 9, 12, 12, 33, 22, 340000), datetime(2024, 9, 12, 12, 33, 22, 340000)], "correct"),
        ([datetime(2025, 9, 12, 12, 33, 22, 340000), datetime(2024, 9, 12, 12, 33, 22, 340000)], "reverse"),
        ([datetime(2024, 8, 12, 12, 33, 22, 340000), datetime(2024, 9, 12, 12, 33, 22, 340000)], "correct"),
        ([datetime(2024, 10, 12, 12, 33, 22, 340000), datetime(2024, 9, 12, 12, 33, 22, 340000)], "reverse"),
        ([datetime(2024, 9, 11, 12, 33, 22, 340000), datetime(2024, 9, 12, 12, 33, 22, 340000)], "correct"),
        ([datetime(2024, 9, 13, 12, 33, 22, 340000), datetime(2024, 9, 12, 12, 33, 22, 340000)], "reverse"),
        ([datetime(2024, 9, 12, 11, 33, 22, 340000), datetime(2024, 9, 12, 12, 33, 22, 340000)], "correct"),
        ([datetime(2024, 9, 12, 13, 33, 22, 340000), datetime(2024, 9, 12, 12, 33, 22, 340000)], "reverse"),
        ([datetime(2024, 9, 12, 12, 32, 22, 340000), datetime(2024, 9, 12, 12, 33, 22, 340000)], "correct"),
        ([datetime(2024, 9, 12, 12, 34, 22, 340000), datetime(2024, 9, 12, 12, 33, 22, 340000)], "reverse"),
        ([datetime(2024, 9, 12, 12, 33, 21, 340000), datetime(2024, 9, 12, 12, 33, 22, 340000)], "correct"),
        ([datetime(2024, 9, 12, 12, 33, 23, 340000), datetime(2024, 9, 12, 12, 33, 22, 340000)], "reverse"),
        ([datetime(2023, 9, 12, 12, 33, 22, 340000), '', datetime(2024, 9, 12, 12, 33, 22, 340000)], "correct"),
        ([datetime(2025, 9, 12, 12, 33, 22, 340000), 'random_str', datetime(2024, 9, 12, 12, 33, 22, 340000)], "reverse"),
        ([datetime(2024, 8, 12, 12, 33, 22, 340000), datetime(2024, 9, 12, 12, 33, 22, 340000)], "correct"),
        ([datetime(2024, 10, 12, 12, 33, 22, 340000), datetime(2024, 9, 12, 12, 33, 22, 340000)], "reverse"),
        (['', datetime(2024, 9, 11, 12, 33, 22, 340000), datetime(2024, 9, 12, 12, 33, 22, 340000), ''], None),
        ([datetime(2024, 9, 11, 12, 33, 22, 340000), datetime(2024, 9, 12, 12, 33, 22, 340000), ''], None),
        (['', datetime(2024, 9, 13, 12, 33, 22, 340000), datetime(2024, 9, 12, 12, 33, 22, 340000)], None),
        ([datetime(2024, 8, 12, 12, 33, 22, 340000), datetime(2024, 9, 12, 12, 33, 22, 350000)], "correct"),
        ([datetime(2024, 9, 12, 12, 33, 22, 350000), datetime(2024, 9, 12, 12, 33, 22, 340000)], "reverse"),
        ([datetime(2024, 9, 12, 12, 33, 22, 340000), datetime(2024, 9, 12, 12, 33, 22, 340000)], None),
        ([datetime(2024, 9, 12, 12, 33, 22, 340000)], None),
        (['', datetime(2024, 9, 12, 12, 33, 22, 340000)], None),
        (['random_string', datetime(2024, 9, 12, 12, 33, 22, 340000)], None),
        ([datetime(2024, 9, 12, 12, 33, 22, 340000), ''], None),
        ([datetime(2024, 9, 12, 12, 33, 22, 340000), 'random_str'], None),
        ([], None),
        (None, None)
    ],
)
def test_date_order(date_list, expected_result):
    assert date_order(date_list) == expected_result # nosec


