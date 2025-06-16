import pytest
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from app.pdf_utils import (
    relu
)


@pytest.mark.parametrize(
    "input, expected_result",
    [
        (-1, 0),
        (0, 0),
        (1, 1)
    ],
)
def test_relu(input, expected_result):
    assert relu(input) == expected_result