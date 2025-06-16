from pdfminer.pdfdocument import PDFPasswordIncorrect
from library.statement_plumber import get_pages
from library.custom_exceptions import NonParsablePDF
from library.utils import match_regex, check_date
from library.utils import keyword_helper


def extract_essential_identity_plumber(path, bank, password):
    """
    Extract identity using plumber (IMP NOTE: Doesn't check for metadata fraud)
    :param: path, bank, password
    :return: dict having identity info, date range, etc
    """

    # NOTE: Works only for federal for now
    if not bank == "federal":
        return {}

    identity_dict = dict()  # stores identity info
    result_dict = dict()  # stores final dictionary to return
    result_dict['is_image'] = False

    try:
        all_pages = get_pages(path, password)
    except NonParsablePDF:
        result_dict['is_image'] = True
        return result_dict
    except PDFPasswordIncorrect:
        result_dict['password_incorrect'] = True
        return result_dict

    if len(all_pages) < 1:
        return {}
    
    all_text = all_pages[0].extract_words()  # extract words from first page

    if len(all_text) < 50:
        # if on first page less than 50 words, its an image
        result_dict['is_image'] = True
        return result_dict

    num_numeric = 0

    name = []
    address = []
    account_number = None
    from_date = None
    to_date = None

    add_address_flag = False  # this is to pen up or down while recording address
    account_number_flag = False  # it its true next word coming will be account number
    sol_mode = False  # if its true means we are going through Branch Sol Id stuff,
    # so it has to ignored while processing address
    # used for third template (mode 4) to count name words, 0 means can start capturing
    name_word_count = -1

    mode = 0
    # 0 for name, 1 for address and account_number (first template),
    # 2 for account_number (second template with no address)
    # 3 implies done capturing
    # 4 for third template

    prev_word = None  # stores the previous word

    global_all_text=""
    for each in all_text:
        word = each['text']
        global_all_text +=" "+ word
        # keep track of numbers found
        numbers_exist = match_regex(word, '.*([0-9]+).*', 1)
        if numbers_exist is not None:
            num_numeric += 1
        
        # skip colons
        if word == ':':
            continue

        # keep track of name
        if mode == 0:
            if word != "Name":
                if word == "Branch":  # end capturing name if Branch word occurs
                    # this template type has both address and acccount number (first template type)
                    mode = 1
                elif word == "Email":  # end capturing name if Email word occurs
                    # this template type has no address but has account number (second template type)
                    mode = 2
                elif word == "and" and prev_word == "Name":
                    # this template has name and address combined (third template type)
                    mode = 4
                elif word == "IFSC:": # end capturing name if IFSC word occurs
                    # this template type has both name and account number (fourth template type federal-jupiter)
                    # account number
                    # account_num_ans = match_regex(global_all_text, "(?i).*AC#\s*([0-9]{14}).*", 1)
                    # account_number = account_num_ans
                    # name
                    ans = match_regex(global_all_text, "(?i).*Account\s*Statement.*\'[0-9]+\s*([A-Za-z\s]+)\s*AC#", 1)
                    if ans == None:
                        ans = match_regex(global_all_text, "(?i).*Account\s*Statement.*\'[0-9]+\s*(\w+\s\w+)", 1)
                    elif ans == None or ans == ' ':
                        ans = match_regex(global_all_text, "(?i).*Account\s*Statement.*AC#\s*[0-9]+\s*([A-Za-z\s]+)\s*IFSC:", 1)
                    name = ans
                    if name is not None:
                        name=name.split(" ")
                    mode = 5
                elif word == "SAVINGS": # end capturing name if SAVINGS word occurs
                    # this template type has both name and account number (fifth template type federal-fi)
                    ans = match_regex(global_all_text, "(?i).*AAccccoouunntt\s*SSttaatteemmeenntt.*to\s*[0-9]+\s*[A-Za-z]+\s*[0-9]+\s*(\w+\s\w+)", 1)
                    name = ans  
                    if name is not None:
                        name=name.split(" ")
                    mode = 6
                elif word == "FEDERAL": # end capturing name if FEDERAL word occurs
                    # this template type has both name and account number (Sixth template type)
                    # account number
                    account_num_ans = match_regex(global_all_text, "(?i).*Statement.*No\s*:?\s*([0-9]{14})\s*Account", 1)
                    account_number = account_num_ans
                    # name
                    ans = match_regex(global_all_text, "(?i).*Statement.*Name\s*:?\s*([A-Za-z\s]+)\s*Federal", 1)
                    name = ans
                    if name is not None:
                        name=name.split(" ")
                    mode = 7
                else:
                    name.append(word)  # capture the name
        
        # keep track of account number (first template type)
        elif mode == 1:
            # hacky code for the template type 1 (two variations)
            if word == "Address" and prev_word == "Communication":
                add_address_flag = True
            elif word == "Number" and prev_word == "Account":
                add_address_flag = False
                account_number_flag = True
            elif account_number_flag:
                account_number = word
                if ':' in account_number:
                    account_number = account_number.split(":")[1]
                account_number_flag = False
            elif add_address_flag:
                if word != "Account":
                    if prev_word == "Branch" and word == "Sol":
                        sol_mode = True
                        address.pop()  # remove the word Branch which was included in address
                    elif sol_mode:
                        if prev_word in ["Id", "ID"]:
                            sol_mode = False
                    elif word == "Customer":
                        add_address_flag = False
                    elif word =="Last":
                        add_address_flag = False
                    else:
                        address.append(word)

        # keep track of account number (second template type): has no address
        elif mode == 2:
            if word == "Statement":
                # account number will be the previous word
                # if current word is "Statement" for second template
                account_number = prev_word
                mode = 3
        
        # for the third template type
        elif mode == 4:
            if word == "holder:":
                name_word_count = 0  # start capturing name from next time
            elif account_number_flag:  # capture account number and end capturing
                account_number = word
                mode = 3
            elif add_address_flag:
                if word == "Account":   # skip Account word
                    pass
                # skip No word if after Account, and capture acc num next time
                elif prev_word == "Account" and word == "No":
                    account_number_flag = True
                else:
                    address.append(word)  # capture address
            elif name_word_count == 3:  # capture address
                add_address_flag = True
                address.append(word)
            elif name_word_count >= 0:  # capture name
                name.append(word)
                name_word_count += 1
        
        # account number for the fifth template type (federal-fi)
        elif mode == 6:
            # if word == match_regex(word, '.*([0-9]{14}).*', 1):
            #     account_number = word
            #     account_number_flag = True
            pass

        if prev_word == "period" and not to_date:
            # Checking if already from_date extracted, only pick first from_date word
            date_obj, _ = check_date(word)
            if date_obj:
                from_date = date_obj.strftime('%Y-%m-%d')
        elif prev_word == "to" and not to_date:
            # Checking if already to_date extracted, only pick first to_date word
            # (Ex- to date word can come in transaction note)
            date_obj, _ = check_date(word)
            if date_obj:
                to_date = date_obj.strftime('%Y-%m-%d')
        prev_word = word

    if num_numeric < 5:
        # if on first page less than 5 numbers, its an image
        result_dict['is_image'] = True
        return result_dict

    if not name:
        name = ""
    name = " ".join(name)
    address = " ".join(address)

    if ':' in name:
        name = name.replace(":", "")
    
    if 'www.federal.co.in' in name:
        try:
            ans = match_regex(global_all_text, "(?i).*Name\s*([A-Za-z\-\.\/\s]+)\sBranch", 1)
        except:
            ans = None    
        name = ans
        
    if not account_number:
        account_number = None
    if not name:
        name = None
    if not address:
        address = None

    # print("Name: ", name)
    identity_dict['account_number'] = account_number
    identity_dict['name'] = name
    identity_dict['address'] = address
    # TODO: Add below three fields for federal
    identity_dict['ifsc'] = None
    identity_dict['micr'] = None
    identity_dict['account_category'] = None
    # identity_dict['is_image']=result_dict['is_image']
    identity_dict['keywords']=keyword_helper(global_all_text)
    print(identity_dict['keywords'])

    result_dict['identity'] = identity_dict

    # set the date range
    if from_date and to_date:
        result_dict['date_range'] = {
            'to_date': to_date, 'from_date': from_date}
    else:
        result_dict['date_range'] = {'to_date': None, 'from_date': None}

    # # old code for bbox based extraction using plumber (super slow)
    #     if get_if_image(path, password):
    #         result_dict['is_image'] = True
    #         return result_dict
    #
    #     # get identity information
    #     identity_dict['account_number'] = get_account_num(path, bank, password)
    #     identity_dict['name'] = get_name(path, bank, password)
    #     identity_dict['address'] = get_address(path, bank, password)
    #     identity_dict['ifsc'] = get_ifsc(path, bank, password)
    #     identity_dict['micr'] = get_micr(path, bank, password)
    #     result_dict['identity'] = identity_dict
    #
    #     # get date range
    #     result_dict['date_range'] = get_date_range(path, bank, password)

    return result_dict
