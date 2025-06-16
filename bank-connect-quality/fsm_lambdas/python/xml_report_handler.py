import xml.etree.ElementTree as ET

import xmltodict

from python.context.logging import LoggingContext
from python.formatted_data_handlers import generate_xml_mappings
from python.configs import LAMBDA_LOGGER, s3, s3_resource, BANK_CONNECT_REPORTS_BUCKET
from python.generate_preferred_xml import generate_raw_xml_mappings, generate_balance_xml_mappings


def xml_report_handler(event, context=None):
    local_logging_context: LoggingContext = LoggingContext(
        source="xml_report_handler"
    )

    entity_id = event.get("entity_id")
    session_flow = event.get("session_flow")
    bank_mapping = event.get("bank_mapping")
    xml_report_version = event.get("xml_report_version")
    is_sme = event.get("is_sme")
    adjusted_eod = event.get("adjusted_eod")
    caching_enabled = event.get("caching_enabled", False)
    session_date_range = event.get("session_date_range", {'from_date': None, 'to_date': None})
    to_reject_account = event.get("to_reject_account", False)
    session_metadata = event.get("session_metadata", {})
    aa_data_file_key = event.get("aa_data_file_key", "")
    bucket_name = event.get("bucket_name")
    xml = None
    xml_original = None
    json_mappings = dict()

    local_logging_context.upsert(
        event=event,
    )

    LAMBDA_LOGGER.info(
        "Inside XML report handler",
        extra=local_logging_context.store
    )

    if not entity_id or not bank_mapping:
        LAMBDA_LOGGER.warning(
            "Payload validation failed",
            extra=local_logging_context.store
        )
        return {"success": False, "message": "payload validation failed"}

    if not session_flow:
        LAMBDA_LOGGER.warning(
            "Session Flow is not enabled",
            extra=local_logging_context.store
        )
        return {"success": False, "message": "Session Flow is not enabled"}

    if not xml_report_version:
        LAMBDA_LOGGER.warning(
            "XML report version not provided",
            extra=local_logging_context.store
        )
        return {"success": False, "message": "XML report version not provided"}

    xml_report_format = session_metadata.get("report_format", "analysis")

    if xml_report_format == "raw":
        json_mappings = generate_raw_xml_mappings(aa_data_file_key, bucket_name, local_logging_context)
    elif xml_report_format == "balance":
        json_mappings = generate_balance_xml_mappings(event, local_logging_context)
    else:
        output_mapping_handler_payload = {
            "entity_id": entity_id,
            "bank_mapping": bank_mapping,
            "is_sme": is_sme,
            "adjusted_eod": adjusted_eod,
            "caching_enabled": caching_enabled,
            "session_flow": session_flow,
            "session_date_range": session_date_range,
            "to_reject_account": to_reject_account
        }

        # Generates XML
        json_mappings = generate_xml_mappings(output_mapping_handler_payload, local_logging_context)

    if not xml:
        xml_original = xmltodict.unparse({"PIR:DATA": json_mappings}) if json_mappings else ""
        xml = generate_xml_from_dict('PIR:Data', json_mappings) if json_mappings else ""

    _ = save_xml_to_s3(f"{entity_id}_original", xml_original)

    # Save XML to S3
    s3_path = save_xml_to_s3(entity_id, xml)
    LAMBDA_LOGGER.info(
        "End of XML report handler",
        extra=local_logging_context.store
    )
    return s3_path


def generate_xml_from_dict(tag, data):
    root = dict_to_xml(tag, data, {"xmlns:PIR": "https://www.finbox.in/"})
    indent(root)
    xml_str = ET.tostring(root, encoding='utf-8', method='xml').decode("utf-8")
    xml_declaration = '<?xml version="1.0" encoding="UTF-8" standalone="no"?>\n'
    return f"{xml_declaration}{xml_str}"


def indent(elem, level=0):
    i = "\n"
    if len(elem):
        if not elem.text or not elem.text.strip():
            elem.text = i
        if not elem.tail or not elem.tail.strip():
            elem.tail = i
        for subelem in elem:
            indent(subelem, level + 1)
        if not elem.tail or not elem.tail.strip():
            elem.tail = i
    else:
        if level and (not elem.tail or not elem.tail.strip()):
            elem.tail = i


def dict_to_xml(tag, data, attribute=None):
    if attribute:
        elem = ET.Element(tag, attribute)
    else:
        elem = ET.Element(tag)
    for key, val in data.items():
        if isinstance(val, dict):
            child = dict_to_xml(key, val)
            elem.append(child)
        elif isinstance(val, list):
            for sub_elem in val:
                child = dict_to_xml(key, sub_elem)
                elem.append(child)
        else:
            elem.set(key, str(val))
    return elem


def save_xml_to_s3(entity_id, xml) -> str:
    s3_object_key = f"xml_report/entity_report_{entity_id}.xml"
    s3_object = s3_resource.Object(BANK_CONNECT_REPORTS_BUCKET, s3_object_key)
    s3_object.put(Body=bytes(xml, encoding="utf-8"))
    s3_path = s3.generate_presigned_url(
        'get_object',
        Params={
            'Bucket': BANK_CONNECT_REPORTS_BUCKET,
            'Key': s3_object_key
        }
    )
    return s3_path
