import re
import fitz
import json
from library.fitz_functions import read_pdf, get_text_in_box

def get_template_data_for_bbox(bbox, path, password='', page_num=0, regex="(?i)(.*)"):

    doc = fitz.Document(path)
    if doc.needs_pass:
        is_password_correct = doc.authenticate(password=password) != 0
        if not is_password_correct:
            return "Password is incorrect"

    doc = read_pdf(path, password)
    page = doc[page_num]
    all_text = get_text_in_box(page, bbox)

    if all_text is not None:
        all_text = all_text.replace('\n', '').replace('(cid:9)', '')

    # print("All text", all_text)
    text = ''
    if regex is not None and all_text is not None:
        regex_match = re.match(regex, all_text)
        if regex_match is not None:
            # print("GROUPS -> ", regex_match.groups(), " -> ", len(regex_match.groups()))
            try:
                text = regex_match.group(1)
            except IndexError as e:
                print(e)
    # print("\n\"", all_text, "\" -->", regex, "-->", text)
    return re.sub(r'(\n|\s)+', ' ', text)