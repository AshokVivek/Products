from itertools import groupby
from operator import itemgetter
import fitz
import pymupdf
import re
from copy import deepcopy

"""Return a Python list of lists created from the words in a fitz.Document page,
depending on a rectangle and an optional list of column (horizontal) coordinates.

Limitations
------------
(1)
Works correctly for simple, non-nested tables only.

(2)
Line recognition depends on coordinates of the detected words in the rectangle.
These will be round to integer( = pixel) values. However, use of different fonts,
scan inaccuracies, etc. may lead to artefacts line differences, which must
be handled by the caller.

Dependencies
-------------
PyMuPDF v1.12.0 or later

Changes
--------
v1.12.0: SQLite and JSON are no longer required.

License
--------
GNU GPL 3.0

(c) 2017-2018 Jorj. X. McKie
"""


# ==============================================================================
# Function ParseTab - parse a document table into a Python list of lists
# ==============================================================================
def parse_table(page, bbox, columns=None, image_flag=None, range_involved=None, inconsistent_regexes = [], match_field = '', plumber_page=None, account_delimiter_regex=[], extract_multiple_accounts=False, round_off_coordinates=False):
    ''' Returns the parsed table of a page in a PDF / (open) XPS / EPUB document.
    Parameters:
    page: fitz.Page object
    bbox: containing rectangle, list of numbers [xmin, ymin, xmax, ymax]
    columns: optional list of column coordinates. If None, columns are generated
    Returns the parsed table as a list of lists of strings.
    The number of rows is determined automatically
    from parsing the specified rectangle.
    '''
    tab_rect = fitz.Rect(bbox).irect
    xmin, ymin, xmax, ymax = tuple(tab_rect)
    inconsistent_yc = False
    list_of_y = []
    if tab_rect.is_empty or tab_rect.is_infinite:
        print("Warning: incorrect rectangle coordinates!")
        return [], list_of_y, inconsistent_yc

    if type(columns) is not list or columns == []:
        coltab = [tab_rect.x0, tab_rect.x1]
    else:
        coltab = sorted(columns)

    if xmin < min(coltab):
        coltab.insert(0, xmin)
    if xmax > coltab[-1]:
        coltab.append(xmax)

    words, inconsistent_yc = get_final_words_list(page, image_flag, inconsistent_regexes, match_field, extract_multiple_accounts, round_off_coordinates)

    if words == []:
        print("Warning: page contains no text")
        return [], list_of_y, inconsistent_yc
    
    if extract_multiple_accounts:
        words = get_words_with_account(words, account_delimiter_regex)
    
    alltxt = []

    # get words contained in table rectangle and distribute them into columns
    for w in words:
        ir = fitz.Rect(w[:4]).irect  # word rectangle
        if ir in tab_rect:
            cnr = 0  # column index
            for i in range(1, len(coltab)):  # loop over column coordinates
                if ir.x0 < coltab[i]:  # word start left of column border
                    cnr = i - 1
                    break
            if extract_multiple_accounts:
                txt = [ir.x0, ir.y0, ir.x1, cnr, w[4], w[8], w[9]]
            else:
                txt = [ir.x0, ir.y0, ir.x1, cnr, w[4]]
            
            if txt not in alltxt:
                alltxt.append(txt)

    if alltxt == []:
        print("Warning: no text found in rectangle!")
        return [], list_of_y, inconsistent_yc

    alltxt.sort(key=itemgetter(1))  # sort words vertically # sorted on the basis of y co-ordinate (here y is constant)

    # create the table / matrix
    spantab = []  # the output matrix

    if range_involved:
        # grouping is flexible by 1 unit along y-axis
        groups = [(y, list(zeile))for y , zeile in groupby(alltxt, itemgetter(1))]
        final_groups = []
        for index in range(len(groups)):
            try:
                if groups[index][0] == groups[index+1][0] - 1:
                    final_groups.append((groups[index][1][0][1], list(groups[index][1]) + list(groups[index+1][1])))
                    del groups[index+1]
                else:
                    final_groups.append((groups[index][1][0][1], list(groups[index][1])))
            except Exception as e:
                continue
        spantab, list_of_y = return_table(final_groups, coltab, extract_multiple_accounts)
    else:
        final_groups = groupby(alltxt, itemgetter(1))
        spantab, list_of_y = return_table(final_groups, coltab, extract_multiple_accounts)

    return spantab, list_of_y, inconsistent_yc

def return_table(final_groups, coltab, extract_multiple_accounts=False):
    table, y_list = [], []
    all_account_number = []
    all_account_category = []
    for y, zeile in final_groups:
        schema = [""] * (len(coltab) - 1)
        current_account_number = []
        current_account_category = []
        for c, words in groupby(zeile, itemgetter(3)):
            entry = ""
            current_account_number = ""
            current_account_category = ""
            for w in words:
                entry = entry + " " + w[4]
                if extract_multiple_accounts:
                    current_account_number = current_account_number + "$ $" + w[5]
                    current_account_category = current_account_category + "$ $" + w[6]
            entry = entry.strip()
            current_account_number = current_account_number.strip("$ $")
            current_account_category = current_account_category.strip("$ $")
            schema[c] = entry

        table.append(schema)
        y_list.append(y)
        if extract_multiple_accounts:
            all_account_number.append(current_account_number)
            all_account_category.append(current_account_category)
    if extract_multiple_accounts:
        final_account_number_list = get_final_list(all_account_number)
        final_account_category_list = get_final_list(all_account_category)

        table = [row + [final_account_number_list[i], final_account_category_list[i]] for i, row in enumerate(table)]
    
    return table, y_list

def memoize_get_regex_top_words(f):
    memory = {}
    
    def inner(page, image_flag, regex_list, match_field, extract_multiple_accounts, round_off_coordinates):
        num = '{}{}{}{}{}{}'.format(page, image_flag, regex_list, match_field, extract_multiple_accounts, round_off_coordinates)
        if num not in memory:
            memory[num] = f(page, image_flag, regex_list, match_field, extract_multiple_accounts, round_off_coordinates)
        return deepcopy(memory[num])
    
    return inner

@memoize_get_regex_top_words
def get_final_words_list(page, image_flag, regex_list, match_field, extract_multiple_accounts, round_off_coordinates):

    if image_flag:
        from library.hsbc_ocr import get_text_words_ocr
        words = get_text_words_ocr(page)
    else: 
        try:
            words = page.get_text_words(flags=pymupdf.TEXTFLAGS_WORDS)
        except Exception as _:
            words = []

    top = None
    string = ""
    string_list = []
    inconsistent_yc = False
    
    if round_off_coordinates:
        rounded_off_words = []
        for word in words:
            word = list(word)
            word[1] = round(word[1], 3)
            rounded_off_words.append(tuple(word))
        words = rounded_off_words
    
    # sorting words only if the pdf is not rotated
    if page.derotation_matrix[5] == 0:
        words.sort(key=lambda x: (x[1], x[0]))
    
    if len(regex_list) > 0:
        new_words = []
        temp_words = []
        for word in words:
            if inconsistent_yc:
                break
            y_coordinate = word[1]
            text = word[4]
            if top is None:
                top = y_coordinate
                string += text + " "
                temp_words.append(word)
            elif abs(y_coordinate - top) < 1:
                string += text + " "
                temp_words.append(word)
            else:
                for regex in regex_list:
                    if extract_multiple_accounts and regex.get('is_present_in_account_regex'):
                        continue
                    test_string = (' ').join(string_list)
                    regex = re.compile(regex.get("regex"))
                    for s in [string, test_string]:
                        captured = regex.findall(s)
                        if captured:
                            if captured[0] != match_field:
                                inconsistent_yc = y_coordinate
                                break
                    if inconsistent_yc:
                        break
                if not inconsistent_yc:
                    new_words += temp_words
                    temp_words = [word]
                    string_list.append(string)
                    if len(string_list) > 4:
                        string_list = string_list[1:]
                    string = text + " "
                    top = y_coordinate
        if not inconsistent_yc:
            new_words += temp_words
        words = new_words
    
    return words, inconsistent_yc

def get_horizontal_lines_using_fitz(all_text_df):
    final_horizontal_lines = []
    all_text_df = all_text_df.sort_values(by='top', ascending=True)
    all_text_dict = all_text_df.to_dict('records')
    for i in range(0, len(all_text_dict)):
        if (i < len(all_text_dict) - 1):
            if (abs(all_text_dict[i]['top'] - all_text_dict[i + 1]['top']) < 7):
                continue
            else:
                final_horizontal_lines.append(
                    float(all_text_dict[i]['bottom']))
        elif (i == len(all_text_dict) - 1):
            final_horizontal_lines.append(
                float(all_text_dict[i]['bottom']))
    return final_horizontal_lines

def get_final_list(all_list):
    final_list = []
    if not all_list: 
        return final_list
    final_list = ['']*len(all_list)
    for i, curr_list in enumerate(all_list):
        final_list[i] = curr_list.strip().split('$ $')[0]
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
    current_line = []
    final_words = []
    string_list = []

    current_account_number = ''
    current_account_category = ''
    
    for word in words:
        current_top = word[1]
        if top is None:
            top = current_top
            string += word[4] + " "
            current_line.append(word)
        elif abs(current_top-top<1):
            string += word[4] + " "
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
                        string = ''
                        to_break = True
                        break
                if to_break:
                    break
            current_line_to_append = []
            for item in current_line:
                item += (current_account_number, current_account_category)
                current_line_to_append.append(item)
            current_line = current_line_to_append
            final_words.extend(current_line)
            
            string_list.append(string)
            if len(string_list) > 4:
                string_list = string_list[1:]
            
            string = word[4]
            current_line = [word]
            top = current_top
    
    current_line_to_append = []
    for item in current_line:
        item += (current_account_number, current_account_category)
        current_line_to_append.append(item)
    current_line = current_line_to_append
    final_words.extend(current_line)
    return final_words