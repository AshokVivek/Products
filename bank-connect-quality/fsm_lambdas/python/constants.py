from enum import Enum

TRXN_KEYS_DEFAULT_VALUES = {
    "transaction_type": "",
    "transaction_note": "",
    "chq_num": "",
    "amount": 0.0,
    "balance": 0.0,
    "date": "",
    "transaction_channel": "Other",
    "hash": "",
    "merchant_category": "",
    "description": "",
    "account_id": "",
    "month_year": "",
    "salary_month": "",
    "category": "Others",
    "perfios_txn_category": "",
}

FRAUD_STATUS_TO_FRAUD_TYPE_MAPPING = {
    "FRAUD": "author_fraud",
    "REFER": "date_fraud"
}

FRAUD_TO_CATEGORY_MAPPING = {
    "author_fraud": "Metadata",
    "date_fraud": "Metadata",
    "inconsistent_transaction": "Accounting",
    "negative_balance": "Transactional",
    "tax_100_multiple": "Transactional",
    "min_rtgs_amount": "Transactional",
    "cash_deposit_bank_holiday_transaction": "Transactional",
    "chq_bank_holiday_transaction": "Transactional",
    "outward_cheque_bounce_bank_holiday_transaction": "Transactional",
    "inward_cheque_bounce_bank_holiday_transaction": "Transactional",
    "more_cash_deposits_than_salary": "Behavioural",
    "salary_remains_unchanged": "Behavioural",
    "salary_1000_multiple": "Behavioural",
    "mostly_cash_transactions": "Behavioural",
    "equal_credit_debit": "Behavioural",
    "more_than_15_days_credit": "Behavioural"
}

FRAUD_TYPE_PRECEDENCE_MAPPING = {
    "author_fraud": 1,
    "date_fraud": 2,
    "inconsistent_transaction": 3,
    "negative_balance": 4,
    "tax_100_multiple": 5,
    "min_rtgs_amount": 6,
    "cash_deposit_bank_holiday_transaction": 7,
    "chq_bank_holiday_transaction": 8,
    "outward_cheque_bounce_bank_holiday_transaction": 9,
    "inward_cheque_bounce_bank_holiday_transaction": 10,
    "more_cash_deposits_than_salary": 11,
    "salary_remains_unchanged": 12,
    "salary_1000_multiple": 13,
    "mostly_cash_transactions": 14,
    "equal_credit_debit": 15,
    "more_than_15_days_credit": 16
}

FRAUD_TO_ERROR_MAPPING = {
    "default": {
        "error_code": "FRAUD",
        "error_message": "Detected fraud",
        "need_extra_data_for_message": False
    },
    "author_fraud": {
        "error_code": "METADATA_FRAUD",
        "error_message": "Author fraud detected",
        "need_extra_data_for_message": False
    },
    "date_fraud": {
        "error_code": "METADATA_FRAUD",
        "error_message": "Date fraud detected",
        "need_extra_data_for_message": False
    },
    "inconsistent_transaction": {
        "error_code": "UNPARSABLE",
        "error_message": "Failed to process because of an unparsable statement",
        "need_extra_data_for_message": False
    },
    "negative_balance": {
        "error_code": "TRANSACTIONAL_FRAUD",
        "error_message": "Negative balances detected",
        "need_extra_data_for_message": False
    },
    "tax_100_multiple": {
        "error_code": "TRANSACTIONAL_FRAUD",
        "error_message": "Tax is the multiple of 100",
        "need_extra_data_for_message": False
    },
    "min_rtgs_amount": {
        "error_code": "TRANSACTIONAL_FRAUD",
        "error_message": "Minimum RTGS amount",
        "need_extra_data_for_message": False
    },
    "cash_deposit_bank_holiday_transaction": {
        "error_code": "TRANSACTIONAL_FRAUD",
        "error_message": "Cash deposit on bank holiday",
        "need_extra_data_for_message": False
    },
    "chq_bank_holiday_transaction": {
        "error_code": "TRANSACTIONAL_FRAUD",
        "error_message": "Cheque deposit on bank holiday transaction",
        "need_extra_data_for_message": False
    },
    "outward_cheque_bounce_bank_holiday_transaction": {
        "error_code": "TRANSACTIONAL_FRAUD",
        "error_message": "Outward cheque bounce on bank holiday",
        "need_extra_data_for_message": False
    },
    "inward_cheque_bounce_bank_holiday_transaction": {
        "error_code": "TRANSACTIONAL_FRAUD",
        "error_message": "Inward cheque bounce bank holiday transaction",
        "need_extra_data_for_message": False
    },
    "more_cash_deposits_than_salary": {
        "error_code": "BEHAVIORAL_FRAUD",
        "error_message": "More cash deposits than salary",
        "need_extra_data_for_message": False
    },
    "salary_remains_unchanged": {
        "error_code": "BEHAVIORAL_FRAUD",
        "error_message": "Salary remains unchanged",
        "need_extra_data_for_message": False
    },
    "salary_1000_multiple": {
        "error_code": "BEHAVIORAL_FRAUD",
        "error_message": "Salary is multiple of 1000",
        "need_extra_data_for_message": False
    },
    "mostly_cash_transactions": {
        "error_code": "BEHAVIORAL_FRAUD",
        "error_message": "Mostly cash transactions",
        "need_extra_data_for_message": False
    },
    "equal_credit_debit": {
        "error_code": "BEHAVIORAL_FRAUD",
        "error_message": "Equal credit debit",
        "need_extra_data_for_message": False
    },
    "more_than_15_days_credit": {
        "error_code": "BEHAVIORAL_FRAUD",
        "error_message": "More than 15 days credit",
        "need_extra_data_for_message": False
    },
    "account_number_missing": {
        "error_code": "NULL_ACCOUNT_NUMBER",
        "error_message": "Account number is unavailable or unidentified",
        "need_extra_data_for_message": False
    },
    "incomplete_months_upload": {
        "error_code": "INCOMPLETE_MONTHS_UPLOAD",
        "error_message": "Statement(s) uploaded contain incomplete months. Missing data present for {}",
        "need_extra_data_for_message": True
    },
    "incomplete_dates_upload": {
        "error_code": "INCOMPLETE_DATES_UPLOAD",
        "error_message": "Statement(s) uploaded contain incomplete dates. Missing dates present for {}",
        "need_extra_data_for_message": True
    },
    "incomplete_months": {
        "error_code": "INCOMPLETE_MONTHS",
        "error_message": "Insufficient data to generate report. There are no transactions for {}",
        "need_extra_data_for_message": True
    },
    "no_transactions": {
        "error_code": "NO_TRANSACTIONS",
        "error_message": "No bank transactions in the expected date range",
        "need_extra_data_for_message": False
    }

}


class DMSDocumentType(Enum):
    PDF = "pdf"
    AA = "aa"
    XLSX = "xlsx"


DOCUMENTS_TO_EXTENSION_MAP = {
    DMSDocumentType.PDF.value: "pdf",
    DMSDocumentType.AA.value: "json",
    DMSDocumentType.XLSX.value: "xlsx"
}
