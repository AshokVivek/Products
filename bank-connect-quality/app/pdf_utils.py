from typing import Union
import fitz
from operator import itemgetter
from itertools import groupby

def read_pdf(path, password) -> Union[fitz.Document, int]:
    try:
        doc = fitz.Document(path)
        if doc.needs_pass and doc.authenticate(password=password) == 0:
            # password authentication failed
            return 0
    except RuntimeError:
        # file not found or is not a pdf file
        return -1
    return doc

def relu(x):
    if x > 0:
        return x
    return 0

def get_horizontal_overlap(box1, box2):
    x11, y11, x12, y12 = box1[:4]
    x21, y21, x22, y22 = box2[:4]
    if x11 < x21:
        overlap = relu(x12 - x21) / float(x22 - x11)
    else:
        try:
            overlap = relu(x22 - x11) / float(x12 - x21)
        except ZeroDivisionError:
            overlap = 0.0
    return overlap

def de_dup_words(words):
    present_words = set()
    new_list = list()
    for word in words:
        c_word = (word[0], word[1], word[4].encode('utf-8'))
        if c_word in present_words:
            continue
        else:
            new_list.append(word)
            present_words.add(c_word)

    return new_list

def get_vertical_overlap(box1, box2):
    x11, y11, x12, y12 = box1[:4]
    x21, y21, x22, y22 = box2[:4]
    if y11 < y21:
        overlap = relu(y12 - y21) / float(y22 - y11)
    else:
        try:
            overlap = relu(y22 - y11) / float(y12 - y21)
        except ZeroDivisionError:
            overlap = 0.0
    return overlap

def get_sorted_boxes(words, is_rotated = False):
    prev_word = None
    if is_rotated:
        words.sort(key=lambda k: (k[2], -k[1]))
    else:
        words.sort(key=itemgetter(3, 0))
    for word in words:
        if prev_word is None:
            prev_word = word
            continue
        if get_horizontal_overlap(word, prev_word) < 0.1 and get_vertical_overlap(word, prev_word) > 0.7:
            word[1] = prev_word[1]
            word[3] = prev_word[3]
        prev_word = word
    if is_rotated:
        words.sort(key=lambda k: (k[2], -k[1]))
    else:
        words.sort(key=itemgetter(3, 0))
    return words

def get_text_in_box(page, box):
    is_rotated = False
    if page.derotation_matrix[5] != 0:
        is_rotated = True

    rect = fitz.Rect(box)
    words = page.get_text_words()

    extracted_words = [list(w) for w in words if fitz.Rect(w[:4]) in rect]
    # print("raw words -> ", extracted_words)
    extracted_words = de_dup_words(extracted_words)
    extracted_words = get_sorted_boxes(extracted_words, is_rotated)

    if is_rotated:
        group = groupby(extracted_words, key=itemgetter(2))
    else:
        group = groupby(extracted_words, key=itemgetter(3))

    string_list = list()
    for y1, g_words in group:
        string_list.append(" ".join(w[4] for w in g_words))
    # print("List -> ", string_list)
    return '\n'.join(string_list)