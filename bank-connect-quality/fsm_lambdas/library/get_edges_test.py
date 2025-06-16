import warnings
import pandas as pd

from library.fitz_functions import read_pdf
from library.statement_plumber import get_pages
from pdfminer.pdftypes import PDFNotImplementedError
from pdfplumber import utils
from library.table import get_horizontal_lines_using_fitz
from copy import deepcopy
import numpy
import re
import pymupdf


warnings.simplefilter(action = "ignore", category = FutureWarning)
pd.options.mode.chained_assignment = None


# not in use
# def get_horizontal_lines_2(page, note_x0, note_x1):
#     edges = page.edges
#     all_edge_list = []
#     horizontal_edges_list = utils.filter_edges(edges, "h", min_length=10)
#     horizontal_edges_df = pd.DataFrame(horizontal_edges_list)
#     all_edge_list.extend(horizontal_edges_list)
#     vertical_edges_list = utils.filter_edges(edges, "v", min_length=10)
#     vertical_edges_list_df = pd.DataFrame(vertical_edges_list)
#     vertical_edges_list_df.sort_values(by=['x0'], inplace=True)
#     vertical_edges_list = vertical_edges_list_df.to_dict('records')
#     # TODO handle none and other cases
#     vertical_edges_list_note_df = vertical_edges_list_df[
#         (vertical_edges_list_df['x0'] > note_x0 - 1) & (vertical_edges_list_df['x0'] < note_x1 + 1)]
#     vertical_start = vertical_edges_list_note_df['top'].min()
#     vertical_end = vertical_edges_list_note_df['bottom'].max()
#
#     horizontal_edges_df_filtered = horizontal_edges_df[
#         (horizontal_edges_df['x0'] > note_x0 - 1) & (horizontal_edges_df['x0'] > note_x1 + 1) & (
#             horizontal_edges_df['top'] > vertical_start - 1) & (
#             horizontal_edges_df['bottom'] < vertical_end + 1)]
#     horizontal_edges_df_filtered.sort_values(by=['top'], inplace=True)
#     horizontal_edges_dict = horizontal_edges_df_filtered.to_dict('records')
#     final_horizontal_lines = []
#     for i in range(0, len(horizontal_edges_dict)):
#         if (i < len(horizontal_edges_dict) - 1):
#             if (abs(horizontal_edges_dict[i]['top'] - horizontal_edges_dict[i + 1]['top']) < 5):
#                 continue
#             else:
#                 final_horizontal_lines.append(
#                     float(horizontal_edges_dict[i]['top']))
#         elif (i == len(horizontal_edges_dict) - 1) and (abs(horizontal_edges_dict[i]['top'] - horizontal_edges_dict[i - 1]['top']) < 5):
#             final_horizontal_lines.append(
#                 float(horizontal_edges_dict[i]['top']))
#
#     final_vertical_lines = []
#     for j in range(0, len(vertical_edges_list)):
#         if (j < len(vertical_edges_list) - 1):
#             if (abs(vertical_edges_list[j]['x0'] - vertical_edges_list[j + 1]['x0']) < 5):
#                 continue
#             else:
#                 final_vertical_lines.append(
#                     float(vertical_edges_list[j]['x0']))
#         elif (j == len(vertical_edges_list) - 1) and (abs(vertical_edges_list[j]['x0'] - vertical_edges_list[j - 1]['x0']) < 5):
#             final_vertical_lines.append(float(vertical_edges_list[j]['x0']))
#
#     return final_horizontal_lines, final_vertical_lines
#
# #not in use
# def get_possible_transaction_notes(path, password, note_x0, note_x1, page_num):
#     all_pages = get_pages(path, password)
#     page = all_pages[page_num]
#     # TODO getting all words takes 1.5 seconds out of total 2.5 seconds per template
#     words = page.extract_words()
#     words_df = pd.DataFrame(words)
#     allowed_words_df = words_df[(
#         words_df['x0'] > note_x0 - 5) & (words_df['x1'] < note_x1 + 5)]
#     horizontal_lines_list = get_horizontal_lines(page, note_x0, note_x1)
#     horizontal_lines_list.insert(0, 0)
#     all_possible_notes = []
#     for i in range(0, len(horizontal_lines_list) - 1):
#         possible_notes_df = allowed_words_df[(allowed_words_df['top'] > horizontal_lines_list[i] - 5) & (
#             allowed_words_df['bottom'] < horizontal_lines_list[i + 1] + 5)]
#         all_possible_notes.append(' '.join(possible_notes_df['text'].tolist()))
#
#     return all_possible_notes


def get_horizontal_lines(edges):
    horizontal_edges_list = utils.filter_edges(edges, "h", min_length=10)
    horizontal_edges_list = sorted(horizontal_edges_list, key=lambda i: i['top'])
    final_horizontal_lines = []
    for i in range(0, len(horizontal_edges_list)):
        if (i < len(horizontal_edges_list) - 1):
            if (abs(horizontal_edges_list[i]['top'] - horizontal_edges_list[i + 1]['top']) < 7):
                continue
            else:
                final_horizontal_lines.append(float(horizontal_edges_list[i]['top']))
        elif (i == len(horizontal_edges_list) - 1):
            final_horizontal_lines.append(float(horizontal_edges_list[i]['top']))
            # print final_horizontal_lines
    return final_horizontal_lines


def get_vertical_lines(edges):
    vertical_edges_list = utils.filter_edges(edges, "v", min_length=10)
    vertical_edges_list = sorted(vertical_edges_list, key=lambda i: i['x0'])
    all_vertical_lines = []
    count = 0
    group_min_top = 1000
    group_max_bottom = 0
    for i in range(0, len(vertical_edges_list)):
        if (i < len(vertical_edges_list) - 1):
            if vertical_edges_list[i]['top'] < group_min_top:
                group_min_top = vertical_edges_list[i]['top']

            if vertical_edges_list[i]['bottom'] > group_max_bottom:
                group_max_bottom = vertical_edges_list[i]['bottom']

            if (abs(vertical_edges_list[i]['x0'] - vertical_edges_list[i + 1]['x0']) < 5):
                count = count + 1
            else:
                all_vertical_lines.append(
                    {'value': float(vertical_edges_list[i]['x0']), 'count': count, 'top': group_min_top,
                     'bottom': group_max_bottom})
                count = 0
                group_min_top = 1000
                group_max_bottom = 0

        elif (i == len(vertical_edges_list) - 1) and (abs(vertical_edges_list[i]['x0'] - vertical_edges_list[i - 1]['x0']) < 5):
            all_vertical_lines.append(
                {'value': float(vertical_edges_list[i]['x0']), 'count': count, 'top': group_min_top,
                 'bottom': group_max_bottom}
            )

    all_vertical_lines_count_sorted = sorted(all_vertical_lines, key=lambda i: i['value'])
    all_vertical_lines_count_sorted_df = pd.DataFrame(all_vertical_lines_count_sorted)
    if all_vertical_lines_count_sorted_df.shape[0] == 0:
        return []
    # TODO change median to mode and check the difference
    top_mode = float(all_vertical_lines_count_sorted_df['top'].mode().iloc[0])
    bottom_mode = float(all_vertical_lines_count_sorted_df['bottom'].mode().iloc[0])
    count_mode = float(all_vertical_lines_count_sorted_df['count'].mode().iloc[0])
    count_top_2 = all_vertical_lines_count_sorted_df['count'].value_counts()[:2].index.tolist()
    if len(count_top_2) > 0:
        count_mode = count_top_2[0]
        if count_mode == 0 and len(count_top_2) > 1:
            count_mode = count_top_2[1]
    # print count_mode, top_mode, bottom_mode
    final_vertical_lines = []
    for each in all_vertical_lines_count_sorted:
        # print each['count'], each['top'], each['bottom'], each['value']
        if (each['count'] == 0):
            continue
        elif (abs(each['count'] - count_mode) > 5) & (abs(float(each['top']) - top_mode) > 10) & (
                abs(float(each['bottom']) - bottom_mode) > 10):
            continue
        else:
            final_vertical_lines.append(float(each['value']))
    # TODO merge lines if they are less than 20 points away??
    # print final_vertical_lines
    return final_vertical_lines


def memoize_get_lines(f):
    memory = {}

    def inner(path, password, page_num, horizontal=False, vertical=False, page=None, plumber_page_edges=None):
        num = '{}{}{}{}{}{}{}'.format(path, password, page_num, horizontal, vertical, page, plumber_page_edges)
        if num not in memory:
            memory[num] = f(path, password, page_num, horizontal, vertical, page, plumber_page_edges)
        return deepcopy(memory[num])

    return inner


@memoize_get_lines
def get_lines(path, password, page_num, horizontal=False, vertical=False, page=None, plumber_page_edges=None):
    try:
        horizontal_lines = []
        vertical_lines = []
        if plumber_page_edges is None:
            try:
                plumber_page=get_pages(path, password)[page_num]
                plumber_page_edges = plumber_page.edges
            except Exception as _:
                plumber_page_edges=None
        try:
            if horizontal and vertical:
                horizontal_lines = get_horizontal_lines(plumber_page_edges)
                vertical_lines = get_vertical_lines(plumber_page_edges)
            elif horizontal:
                horizontal_lines = get_horizontal_lines(plumber_page_edges)
            elif vertical:
                vertical_lines = get_vertical_lines(plumber_page_edges)
        except (TypeError, PDFNotImplementedError):
            # for some weird pdfs plumber cannot extract lines and throws these error
            return [], []
    except Exception as _:
        # TODO: handle exact error here
        words = get_words_with_boxes(path, password, page_num, page=page)
        words_df = pd.DataFrame(
            words, columns=['x0', 'x1', 'top', 'bottom', 'text'])
        if horizontal:
            horizontal_lines = get_horizontal_lines_using_fitz(words_df)
            vertical_lines = []

    return horizontal_lines, vertical_lines

def memoize_graphical_lines(f):
    memory = {}

    def inner(path, password, page_num, horizontal=False, vertical=False, inconsistent_regexes = [], match_field = '', bank='', page=None, plumber_page_edges=None, account_delimiter_regex=[], extract_multiple_accounts=False):
        num = '{}{}{}{}{}{}{}{}{}{}{}{}'.format(path, password, page_num, horizontal, vertical, inconsistent_regexes, match_field, bank, page, plumber_page_edges, account_delimiter_regex, extract_multiple_accounts)
        if num not in memory:
            memory[num] = f(path, password, page_num, horizontal, vertical, inconsistent_regexes, match_field, bank, page, plumber_page_edges, account_delimiter_regex, extract_multiple_accounts)
        return deepcopy(memory[num])

    return inner

@memoize_graphical_lines
def get_df_graphical_lines(path, password, page_num, horizontal=False, vertical=False, inconsistent_regexes = [], match_field = '', bank='', page=None, plumber_page_edges=None, account_delimiter_regex=[], extract_multiple_accounts=False):
    if vertical == True:
        vertical_boolean = True
        vertical_lines_input = []
    else:
        vertical_boolean = False
        vertical_lines_input = vertical

    if horizontal == True:
        horizontal_boolean = True
    else:
        horizontal_boolean = False
    
    horizontal_lines, vertical_lines = get_lines(path, password, page_num, horizontal_boolean, vertical_boolean, page=page, plumber_page_edges=plumber_page_edges)

    if horizontal == 'Text':
        horizontal_lines = get_horizontal_lines_text(path, password, page_num, bank, page=page)
    if len(vertical_lines_input) > 0:
        vertical_lines = vertical_lines_input
    
    #for quality ticket solve module
    if isinstance(horizontal, list):
        horizontal_lines = horizontal
    
    words, inconsistent = get_final_words_list(path, password, page_num, page, inconsistent_regexes, match_field, extract_multiple_accounts)
    
    words_df = pd.DataFrame(words, columns=['x0', 'x1', 'top', 'bottom', 'text'])
    
    if extract_multiple_accounts:
        words = get_words_with_account(words, account_delimiter_regex)
        words_df = pd.DataFrame(words, columns=['x0', 'x1', 'top', 'bottom', 'text', 'account_number', 'account_category'])
    
    final_list = pd.DataFrame()
    all_account_number = []
    all_account_category = []
    
    for i in range(len(vertical_lines) - 1):
        all_possible_words = []
        current_account_number = []
        current_account_category = []
        allowed_words_df = words_df[(words_df['x0'] > vertical_lines[i] - 5) & (words_df['x1'] < vertical_lines[i + 1] + 5)]
        
        if inconsistent:
            allowed_words_df = allowed_words_df[allowed_words_df['top'] < inconsistent]
        
        for j in range(len(horizontal_lines) - 1):
            possible_words_df = allowed_words_df[(allowed_words_df['top'] > horizontal_lines[j] - 5) & (allowed_words_df['bottom'] < horizontal_lines[j + 1] + 5)]
            all_possible_words.append(' '.join(possible_words_df['text'].tolist()))
            
            if extract_multiple_accounts:
                current_account_number.append('$ $'.join(possible_words_df['account_number'].tolist()))
                current_account_category.append('$ $'.join(possible_words_df['account_category'].tolist()))
        
        all_account_number.append(current_account_number)
        all_account_category.append(current_account_category)
        final_list[i] = all_possible_words
    
    if extract_multiple_accounts:
        final_account_number_list = get_final_list(all_account_number)
        final_account_category_list = get_final_list(all_account_category)
        final_list['account_number'] = final_account_number_list
        final_list['account_category'] = final_account_category_list
    
    return final_list.values.tolist(), inconsistent

def memoize_get_regex_top_words(f):
    memory = {}
    
    def inner(path, password, page_num, page, regex_list, match_field, extract_multiple_accounts):
        num = '{}{}{}{}{}{}{}'.format(path, password, page_num, page, regex_list, match_field, extract_multiple_accounts)
        if num not in memory:
            memory[num] = f(path, password, page_num, page, regex_list, match_field, extract_multiple_accounts)
        return deepcopy(memory[num])
    
    return inner


@memoize_get_regex_top_words
def get_final_words_list(path, password, page_num, page, regex_list, match_field, extract_multiple_accounts):
    
    words = get_words_with_boxes(path, password, page_num, page=page)
    
    top = None
    string = ""
    inconsistent = False
    string_list = []
    if len(regex_list)>0:
        for items in words:
            if inconsistent:
                break
            current_top = items.get("top")
            if top is None:
                top = current_top
                string += items.get("text") + " "
            elif abs(current_top-top<1):
                string += items.get("text") + " "
            else:
                test_string = (' ').join(string_list)
                for inconsistent_regex in regex_list:
                    if extract_multiple_accounts and inconsistent_regex.get('is_present_in_account_regex'):
                        continue
                    regex = re.compile(inconsistent_regex.get("regex"))
                    for s in [string, test_string]:
                        if captured := regex.findall(s):
                            if captured[0] != match_field:
                                inconsistent = current_top
                                break
                    if inconsistent:
                        break
                
                string_list.append(string)
                if len(string_list) > 4:
                    string_list = string_list[1:]
                
                string = items.get("text") 
                top = current_top
    return words, inconsistent

def get_final_list(all_list):
    final_list = []
    if not all_list: 
        return final_list
    final_list = ['']*len(all_list[0])
    for curr_list in all_list:
        for i, elem in enumerate(curr_list):
            if elem.strip():
                final_list[i] = elem.strip().split('$ $')[0]
    prev_ele = ''
    for i, item in enumerate(final_list):
        if item not in ['', None]:
            prev_ele = item
        final_list[i] = prev_ele
    return final_list


def memoize_get_words_with_account(f):
    memory = {}
    
    def inner(words, account_delimiter_regex):
        num = '{}{}'.format(words, account_delimiter_regex)
        if num not in memory:
            memory[num] = f(words, account_delimiter_regex)
        return deepcopy(memory[num])
    
    return inner

@memoize_get_words_with_account
def get_words_with_account(words, account_delimiter_regex):

    top = None
    string = ""
    string_list = []
    current_line = []
    final_words = []
    
    current_account_number = ''
    current_account_category = ''
    
    for word in words:
        current_top = word.get("top")
        if top is None:
            top = current_top
            string += word.get("text") + " "
            current_line.append(word)
        elif abs(current_top-top<1):
            string += word.get("text") + " "
            current_line.append(word)
        else:
            test_string = (' ').join(string_list)
            for account_regex in account_delimiter_regex:
                regex = re.compile(account_regex.get("regex"))
                for s in [string, test_string]:
                    to_break = False
                    if captured := regex.findall(s):
                        if captured[0] == current_account_number and current_account_category:
                            to_break = True
                            break
                        current_account_number = captured[0]
                        current_account_category = s
                        current_line = []
                        s = ''
                        to_break = True
                        break
                if to_break:
                    break
            for item in current_line:
                item['account_number'] = current_account_number
                item['account_category'] = current_account_category
            final_words.extend(current_line)

            string_list.append(string)
            if len(string_list) > 4:
                string_list = string_list[1:]
            string = word.get("text")
            current_line = [word]
            top = current_top
    
    current_line_to_append = []
    for item in current_line:
        item['account_number'] = current_account_number
        item['account_category'] = current_account_category
        current_line_to_append.append(item)
    current_line = current_line_to_append
    final_words.extend(current_line)

    return final_words

def get_words_with_boxes(path, password, page_number=1, page=None):
    if not page:
        doc = read_pdf(path, password)
        if isinstance(doc, int):
            return []
        page = doc[page_number]
    is_pdf_rotated = False
    if page.derotation_matrix[5] != 0:
        print("pdf is rotated")
        is_pdf_rotated = True
    # print("is_pdf_rotated ", is_pdf_rotated)
    try:
        text_words = page.get_text_words(flags=pymupdf.TEXTFLAGS_WORDS)
    except Exception as _:
        text_words = []
    
    final_words = list()
    for word in text_words:
        x0, top, x1, bottom, text, _, _, _ = word
        if is_pdf_rotated:
            x0_tmp = x0
            x1_tmp = x1
            top_tmp = top
            bottom_tmp = bottom
            
            x0 = top_tmp
            x1= bottom_tmp
            top = x0_tmp
            bottom = x1_tmp
        w_dict = {'text': text, 'x0': x0, 'x1': x1, 'top': top, 'bottom': bottom}
        if w_dict not in final_words:
            final_words.append(w_dict)
    
    # sorting words only if the pdf is not rotated
    if not is_pdf_rotated:
        final_words.sort(key=lambda x: (x['top'], x['x0']))
    return final_words


def get_horizontal_lines_text(path, password, page_number=1, bank='', page=None):
    words = get_words_with_boxes(path, password, page_number, page=page)
    words_df = pd.DataFrame(words).sort_values(by='top')
    words_df['word_height'] = words_df['bottom'] - words_df['top']
    word_height = words_df['word_height'].median()
    horizontal_edges_list = words_df.bottom.unique()
    
    if words_df.shape[0] > 0:
        top_line =  words_df['top'].min()
        if top_line not in horizontal_edges_list:
            horizontal_edges_list = numpy.insert(horizontal_edges_list, 0, top_line)
    
    final_horizontal_lines = []
    for i in range(0, len(horizontal_edges_list)):
        if (i < len(horizontal_edges_list) - 1):
            horizontal_diff = abs(horizontal_edges_list[i + 1] - horizontal_edges_list[i])
            maintainable_diff = horizontal_diff - word_height
            if maintainable_diff < 0 and abs(maintainable_diff/horizontal_diff)>0.001:
                continue
            else:
                final_horizontal_lines.append(float(horizontal_edges_list[i]))
        elif i == (len(horizontal_edges_list) - 1):
            final_horizontal_lines.append(float(horizontal_edges_list[i]))

    new_final = []
    for each in final_horizontal_lines:
        each = int(each)
        new_final.append(each)
    return new_final

def all_text_check_for_pdf(path, password, page_num):
    """
        This function returns a boolean based on all text of all pages after excluding a certain text pattern.
        If all text of all pages is same then we return True, else False.
    """

    doc = read_pdf(path, password)
    
    if isinstance(doc, int):
        return False
    
    all_text = []
    
    for i in range(doc.page_count):
        text = doc[i].get_text()
        text = text.replace(' ', '')
        t = f'page{i + 1}/{doc.page_count}'
        text = text.replace('\n','').replace(t, '')
        all_text.append(text)
    
    for i in range(len(all_text)):
        if all_text[i] != all_text[page_num]:
            return False
    
    return True

def get_last_page_regex_simulation(doc, page_num, template_json):
    
    words = get_words_with_boxes(None, '', page_num, page=doc[page_num])
    regex_list = template_json
    top = None
    string = ""
    inconsistent = False
    string_list = []
    match_field = "10101010010101001010101001"
    captured_data = None
    combined_text_strings = []
    text_strings = []
    if len(regex_list)>0:
        for items in words:
            if inconsistent:
                break
            current_top = items.get("top")
            if top is None:
                top = current_top
                string += items.get("text") + " "
            elif abs(current_top-top<1):
                string += items.get("text") + " "
            else:
                test_string = (' ').join(string_list)
                for inconsistent_regex in regex_list:
                    regex = re.compile(inconsistent_regex.get("regex"))
                    combined_text_strings.append(test_string)
                    text_strings.append(string)
                    for s in [string, test_string]:
                        if captured := regex.findall(s):
                            if captured[0] != match_field:
                                captured_data = captured[0]
                                inconsistent = current_top
                                break
                    if inconsistent:
                        break
                
                string_list.append(string)
                if len(string_list) > 4:
                    string_list = string_list[1:]
                
                string = items.get("text") 
                top = current_top
    
    last_page_coordinate = inconsistent
    
    return last_page_coordinate, captured_data, text_strings, combined_text_strings