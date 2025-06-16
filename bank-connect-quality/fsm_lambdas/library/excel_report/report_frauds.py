from library.excel_report.report_formats import transaction_format_func, months_formats_func
from library.excel_report.report_transactions import write_transaction_column, transaction_column_names, transaction_column_names_single_category, SingleCategoryTransactionsColumns, write_transaction_with_single_category
from library.fraud import frauds_priority_list, fraud_category
from xlsxwriter.utility import xl_rowcol_to_cell
import warnings
import pandas as pd


warnings.simplefilter(action = "ignore", category = FutureWarning)
pd.options.mode.chained_assignment = None


def hyper_link_cells(work_sheet, row, field, link_address, months_formats, num_transactions, typ, workbook_num=''):
    sheet_name = f"internal:'Frauds Transactions{workbook_num}'!" if workbook_num else "internal:'Frauds Transactions'!"
    cell = xl_rowcol_to_cell(link_address[0], link_address[1])
    work_sheet.write_url(row, 0, sheet_name + str(cell), months_formats['vertical_heading_cell'], string=typ)
    work_sheet.write_url(row, 1, sheet_name + str(cell), months_formats['left_text_box_cell'], string=field['description'])
    work_sheet.write_url(row, 2, sheet_name + str(cell), months_formats['text_box_cell'], string=fraud_category[field['fraud_type']].title())
    work_sheet.write_url(row, 3, sheet_name + str(cell), months_formats['text_box_cell'], string='Yes')
    work_sheet.write_url(row, 4, sheet_name + str(cell), months_formats['button_cell'], string="{} transactions ->".format(num_transactions))


def frauds_func(workbook, frauds_list, hash_to_index, transaction_data, version, personal_data, country='IN', workbook_num=''):
    months_formats = months_formats_func(workbook)
    transaction_formats = transaction_format_func(workbook)
    work_sheet = workbook.add_worksheet('Frauds'+workbook_num)
    work_sheet.write('A1', 'Frauds', months_formats['primary_heading'])
    
    # commented out because we want to still make a frauds page with all `NO`(s)
    # if frauds_list == []:
    #     return

    # cast different metadata fraud types to explicit author fraud
    for index in range(len(frauds_list)):
        if frauds_list[index].get("fraud_type", None) in ["good_author_fraud","rgb_fraud",
        "identity_name_fraud","font_and_encryption_fraud","page_hash_fraud","tag_hex_fraud","flag_000rg_50_fraud",
        "tag_hex_on_page_cnt_fraud","TD_cnt_fraud","TJ_cnt_fraud","touchup_textedit_fraud",
        "cnt_of_pagefonts_not_equal_fraud","good_font_type_size_fraud","pikepdf_exception", "Tj_null_cnt_fraud", "Non_hex_fraud"
        ]:
            frauds_list[index]["fraud_type"]="author_fraud"
    
    if version=='v2':
        df = pd.DataFrame(transaction_data)
        df_sum = df.groupby("transaction_type").sum()
        total_debited = df_sum['amount'].get('debit', 0)
        total_credited = df_sum['amount'].get('credit', 0)
        if transaction_data[0]['transaction_type'] == 'debit':
            total_debited = total_debited - transaction_data[0]['amount']
        if transaction_data[0]['transaction_type'] == 'credit':
            total_credited = total_credited - transaction_data[0]['amount']

        balance = transaction_data[0]['balance'] + total_credited - total_debited
        if float(round(balance, 2)) != float(round(transaction_data[-1]['balance'], 2)):
            print("For V2, computed is not matching, Marking Fraud for inconsistent transactions")
            frauds_list.append({
                "fraud_type": "inconsistent_transaction"
            })

    # cleaning frauds_list for filtering invalid/unfound hashes
    cleaned_frauds_list = []
    for fraud in frauds_list:
        transaction_hash = fraud.get("transaction_hash", None)
        if transaction_hash:
            if transaction_hash in hash_to_index:
                cleaned_frauds_list.append(fraud)
        else:
            # case when transaction_hash key is missing
            # null transaction hashes are also added to cleaned_frauds_list
            cleaned_frauds_list.append(fraud)
    
    frauds_list = cleaned_frauds_list

    frauds_dict = {"order": []}

    for fraud in frauds_list:
        if fraud["fraud_type"] not in frauds_dict:
            frauds_dict[fraud["fraud_type"]] = []
            frauds_dict['order'].append(fraud["fraud_type"])
        if fraud.get("transaction_hash"):
            frauds_dict[fraud["fraud_type"]].append(fraud.get("transaction_hash"))

    work_sheet_transact = workbook.add_worksheet('Frauds Transactions'+workbook_num)
    work_sheet_transact.write('A1', 'Frauds Transactions', months_formats['primary_heading'])

    transact_row = 3
    hyperlink_dict = {}
    for key in frauds_dict['order']:
        if not frauds_dict[key]:
            continue
        k = key
        k = k.replace("_", " ")
        k = k.title()
        work_sheet_transact.write(transact_row, 0, k, months_formats['primary_heading'])
        hyperlink_dict[key] = [transact_row, 0]
        transact_row += 1
        if version=='v5':
            transaction_column_names_single_category(transact_row, work_sheet_transact, transaction_formats, headings_key=SingleCategoryTransactionsColumns.get_columns(SingleCategoryTransactionsColumns.CREDIT_DEBIT_IN_ONE_ROW_WITH_TYPE))
        else:
            transaction_column_names(transact_row, work_sheet_transact, transaction_formats)
        transact_row += 1
        for hashs in frauds_dict[key]:
            if version=='v5':
                write_transaction_with_single_category(work_sheet_transact, transact_row, transaction_data[hash_to_index[hashs]], transaction_formats, SingleCategoryTransactionsColumns.get_columns(SingleCategoryTransactionsColumns.CREDIT_DEBIT_IN_ONE_ROW_WITH_TYPE), personal_data)
            else:
                write_transaction_column(work_sheet_transact, transact_row, transaction_data[hash_to_index[hashs]], transaction_formats, 1)
            transact_row += 1

        transact_row += 3

    row = 0
    row += 3
    work_sheet.write(row, 0, 'Fraud Type', transaction_formats['horizontal_heading_cell'])
    work_sheet.write(row, 1, 'Description', transaction_formats['horizontal_heading_cell'])
    work_sheet.write(row, 2, 'Fraud Category', transaction_formats['horizontal_heading_cell'])
    work_sheet.write(row, 3, 'Yes/No', transaction_formats['horizontal_heading_cell'])
    row += 1
    # Removing frauds that are not required for Indonesia
    unnecessary_fraud_types_ID = ['more_cash_deposits_than_salary',
                                  'salary_remains_unchanged', 
                                  'salary_1000_multiple',
                                  'negative_balance',
                                  'tax_100_multiple',
                                  'min_rtgs_amount'
                                ]
    ro=0
    for field in frauds_priority_list:
        if country in ['ID'] and field['fraud_type'] in unnecessary_fraud_types_ID:
            continue
        typ = field['fraud_type']
        typ = typ.replace("_", " ")
        typ = typ.title()

        if field['fraud_type'] in frauds_dict:
            if field['fraud_type'] in hyperlink_dict and len(frauds_dict[field['fraud_type']]) > 0:
                hyper_link_cells(work_sheet, row+ro, field,  hyperlink_dict[field['fraud_type']], months_formats, len(frauds_dict[field['fraud_type']]), typ, workbook_num)
                ro +=1  # TODO : This is a hotfix where the inconsistent key is overlapped in the report field. This has to be sorted.
                continue
            else:
                work_sheet.write(row+ro, 0, typ, months_formats['vertical_heading_cell'])
            work_sheet.write(row+ro, 3, "Yes", months_formats['text_box_cell'])
        else:
            work_sheet.write(row+ro, 0, typ, months_formats['vertical_heading_cell'])
            work_sheet.write(row+ro, 3, "No", months_formats['text_box_cell'])
        work_sheet.write(row+ro, 1, field['description'], months_formats['left_text_box_cell'])
        fraud_cate = fraud_category[field['fraud_type']]
        fraud_cate = fraud_cate.title()
        work_sheet.write(row+ro, 2, fraud_cate, months_formats['text_box_cell'])
        ro+=1   # TODO : This has to be removed too

    size_fraud = [['A', 35], ['B', 70], ['C', 20], ['D', 15], ['E', 15]]
    for i in range(len(size_fraud)):
        s = '{}:{}'.format(size_fraud[i][0], size_fraud[i][0])
        work_sheet.set_column(s, size_fraud[i][1])