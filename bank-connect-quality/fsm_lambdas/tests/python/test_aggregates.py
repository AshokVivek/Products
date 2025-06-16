import pytest

from python.aggregates import (
    is_castable_into_int,
    is_empty
)


@pytest.mark.parametrize("input, expected_output", [
    (None, False),
    ('', False),
    (3.14, True),
    (-3.14, True),
    ('3.14', False),
    ('abc', False),
    (True, True),
    (False, True),
    (3, True),
    (-3, True),
    (0, True)
])
def test_is_castable_into_int(input, expected_output):
    assert is_castable_into_int(input) is expected_output

@pytest.mark.parametrize("input, expected_output", [
    (1, False),
    (-1, False),
    (1.1, False),
    (-1.1, False),
    (False, False),
    (True, False),
    ('1', False),
    (-1, False),
    ([], False),
    ('', True),
    (None, True)
])
def test_is_empty(input, expected_output):
    assert is_empty(input) is expected_output