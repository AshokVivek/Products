import pytest
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from python.perfios_integration_iifl_handlers import (
    _get_perfios_xml_report_origin,
    _get_origin_recalibrated_perfios_xml_report
)


@pytest.mark.parametrize(
    "perfios_xml_report_dict, expected_result",
    [
        ({"PIR:Data":      {"Key_A": "Value_A", "Key_B": "Value_B"}}, "PIR:Data"),
        ({"IIFLXMLRoot":   {"Key_A": "Value_A", "Key_B": "Value_B"}}, "IIFLXMLRoot"),
        ({"IIFLXML":       {"Key_A": "Value_A", "Key_B": "Value_B"}}, "IIFLXML"),
        ({"PIR:Data":      {"Key_A": "Value_A"}}, "PIR:Data"),
        ({"IIFLXMLRoot":   {"Key_A": "Value_A"}}, "IIFLXMLRoot"),
        ({"IIFLXML":       {"Key_A": "Value_A"}}, "IIFLXML"),
        ({"PIR:Data":      "Random STR value"}, None),
        ({"IIFLXMLRoot":   "Random STR value"}, None),
        ({"IIFLXML":       "Random STR value"}, None),
        ({"root": {"Key_A": "Some STR value"}}, None),
        ({"Not_Known_Key": {"Key_A": "Some STR value"}}, None),
        ("Not_Dict", None),
        ("", None),
        (5, None),
        ({}, None),
        (None, None),
    ],
)
def test__get_perfios_xml_report_origin(perfios_xml_report_dict, expected_result):
    assert _get_perfios_xml_report_origin(perfios_xml_report_dict) == expected_result

@pytest.mark.parametrize(
    "perfios_xml_report_dict, expected_result",
    [
        ({"PIR:Data":      {"Key_A": "Value_A", "Key_B": "Value_B"}}, {"Key_A": "Value_A", "Key_B": "Value_B"}),
        ({"IIFLXMLRoot":   {"Key_A": "Value_A", "Key_B": "Value_B"}}, {"Key_A": "Value_A", "Key_B": "Value_B"}),
        ({"IIFLXML":       {"Key_A": "Value_A", "Key_B": "Value_B"}}, {"Key_A": "Value_A", "Key_B": "Value_B"}),
        ({"PIR:Data":      {"Key_A": "Value_A"}}, {"Key_A": "Value_A"}),
        ({"IIFLXMLRoot":   {"Key_A": "Value_A"}}, {"Key_A": "Value_A"}),
        ({"IIFLXML":       {"Key_A": "Value_A"}}, {"Key_A": "Value_A"}),
        ({"PIR:Data":      "Random STR value"}, {"PIR:Data":      "Random STR value"}),
        ({"IIFLXMLRoot":   "Random STR value"}, {"IIFLXMLRoot":   "Random STR value"}),
        ({"IIFLXML":       "Random STR value"}, {"IIFLXML":       "Random STR value"}),
        ({"Not_Known_Key": {"Key_A": "Some STR value"}}, {"Not_Known_Key": {"Key_A": "Some STR value"}}),
        ("Not_Dict", "Not_Dict"),
        ({"": "Not_Dict"}, {"": "Not_Dict"}),
        ({"": {"Key_A": "Value_A"}}, {"": {"Key_A": "Value_A"}}),
        (5, 5),
        ({}, {}),
        (None, None),
        ({"root": {"PIR:Data":      {"Key_A": "Value_A", "Key_B": "Value_B"}}}, {"Key_A": "Value_A", "Key_B": "Value_B"}),
        ({"root": {"IIFLXMLRoot":   {"Key_A": "Value_A", "Key_B": "Value_B"}}}, {"Key_A": "Value_A", "Key_B": "Value_B"}),
        ({"root": {"IIFLXML":       {"Key_A": "Value_A", "Key_B": "Value_B"}}}, {"Key_A": "Value_A", "Key_B": "Value_B"}),
        ({"root": {"PIR:Data":      {"Key_A": "Value_A"}}}, {"Key_A": "Value_A"}),
        ({"root": {"IIFLXMLRoot":   {"Key_A": "Value_A"}}}, {"Key_A": "Value_A"}),
        ({"root": {"IIFLXML":       {"Key_A": "Value_A"}}}, {"Key_A": "Value_A"}),
        ({"root": {"root": {"PIR:Data":      "Random STR value"}}}, {"PIR:Data":      "Random STR value"}),
        ({"root": {"root": {"IIFLXMLRoot":   "Random STR value"}}}, {"IIFLXMLRoot":   "Random STR value"}),
        ({"root": {"root": {"IIFLXML":       "Random STR value"}}}, {"IIFLXML":       "Random STR value"}),
        ({"root": {"PIR:Data":      "Random STR value"}}, {"PIR:Data":      "Random STR value"}),
        ({"root": {"IIFLXMLRoot":   "Random STR value"}}, {"IIFLXMLRoot":   "Random STR value"}),
        ({"root": {"IIFLXML":       "Random STR value"}}, {"IIFLXML":       "Random STR value"}),
        ({"root": {"Not_Known_Key": {"Key_A": "Some STR value"}}}, {"Not_Known_Key": {"Key_A": "Some STR value"}}),
        ({"root": "Not_Dict"}, {"root": "Not_Dict"}),
        ({"root": {"": "Not_Dict"}}, {"": "Not_Dict"}),
        ({"root": {"": {"Key_A": "Value_A"}}}, {"": {"Key_A": "Value_A"}}),
        ({"root": 5}, {"root": 5}),
        ({"root": {}}, {}),
        ({"root": None}, {"root": None}),
    ]
)
def test__get_origin_recalibrated_perfios_xml_report(perfios_xml_report_dict, expected_result):
    assert _get_origin_recalibrated_perfios_xml_report(perfios_xml_report_dict) == expected_result
