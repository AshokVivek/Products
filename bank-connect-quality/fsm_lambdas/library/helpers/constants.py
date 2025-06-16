DEFAULT_TIMESTAMP_UTC = "1970-01-01 00:00:00"
DEFAULT_BALANCE_STRING = "10101010101010101010101010101"
DEFAULT_BALANCE_FLOAT = float(10101010101010101010101010101)
DEFAULT_DATE = "01/01/1970"

# For this list, this order matters, please do not change
CREDIT_CARD_TYPE_WORDS_TO_REMOVE = ["Default E", "Default"]
CREDIT_CARD_TYPE_WORDS_TO_REMOVE_STARTING = ["0", "1", "2", "OLD"]
CREDIT_CARD_TYPE_WORDS_TO_REMOVE_END = ["B", "E"]
CREDIT_CARD_TYPE_WORDS_TO_REMOVE_ANYWHERE = ["BANK", "CREDIT", "CARD", "STATEMENT"]


BCABNK_COMPLETE_TRSNSACTION_TOLERANCE = 1

DEFAULT_LEAP_YEAR = "1972"


class TRANSACTION_CHANNELS:
    BANK_CHARGE = "bank_charge"
    SALARY = "salary"
    REFUND = "refund"
    REVERSAL = "reversal"
    CASH_WITHDRAWL = "cash_withdrawl"
    CASH_DEPOSIT = "cash_deposit"
    OUTWARD_CHEQUE_BOUNCE = "outward_cheque_bounce"
    AUTO_DEBIT_PAYMENT_BOUNCE = "auto_debit_payment_bounce"
    INWARD_CHEQUE_BOUNCE = "inward_cheque_bounce"
    INWARD_PAYMENT_BOUNCE = "inward_payment_bounce"


BOUNCE_TRANSACTION_CHANNELS = [
    TRANSACTION_CHANNELS.OUTWARD_CHEQUE_BOUNCE,
    TRANSACTION_CHANNELS.AUTO_DEBIT_PAYMENT_BOUNCE,
    TRANSACTION_CHANNELS.INWARD_CHEQUE_BOUNCE,
    TRANSACTION_CHANNELS.INWARD_PAYMENT_BOUNCE
]

FEB_29TH_REGEXES = [
    "29[\/\-\.\/ ]*(?:February|FEBRUARY|FEB|Feb)\-?",
    "(?:February|FEBRUARY|Feb|FEB)[\/\-\.\/ ]*29\-?",
    "29[\/\-\.\/ ]*02\-?",
    "02[\/\-\.\/ ]*29\-?"
]

LOAN_TYPES = [
    "home_loan",
    "gold_loan",
    "auto_loan",
    "personal_loan",
    "business_loan",
    "term_loan"
]

UJJIVAN_IGNORE_TRANSACTIONS_NOTE = ["Account Type", "Summary"]
BANKS_WITH_TRANSACTIONS_SPLIT_ENABLED = ["ujjivan", "bhadradri_urban", "vasai", "equitas", "chaitanya_godavari"]
HEADERS_OPENING_BALANCE = ["Opening Balance", "Balance Brought Forward", "B/F", "Balance B/F", "Final balance:", "BF ..."]
HEADERS_CLOSING_BALANCE = ["Closing Balance", "Total / Balance", "Balance C/F", "Total"]

BOUNCE_SINGLE_CATEGORIES_TRANSACTIONS = [
    "Below Min Balance",
    "Bounced I/W Cheque",
    "Bounced I/W Cheque",
    "Bounced O/W Cheque",
    "Bounced I/W ECS",
    "Bounced I/W Payment",
    "Bounced O/W ECS",
    "Bounced O/W Payment"
]

BOUNCE_SINGLE_CATEGORIES_CHARGES = [
    "Bounced I/W Cheque Charges",
    "Bounced I/W ECS Charges",
    "Bounced O/W Cheque Charges",
    "Bounced O/W ECS Charges",
    "Bounced O/W Payment Charges",
    "Bounced I/W Payment Charges"
]

BANK_CHARGE_AMOUNTS = [24.78, 11.8, 9.43, 9.44, 24.78, 12.98, 4.26, 4.25, 1.89, 1.9]

MIN_SALARY_AMOUNT_FOR_KEYWORD_CLASSIFICATION = 3000

CUT_FOOTER_REGEXES = {
    "equitas": ["(?i)[\S\s]*page\s*[0-9]*\s*of\s*[0-9]\s*"],
    "chaitanya_godavari": ["(?i)[\S\s]*This is a system generated statement, no"]
}

CUT_HEADERS_PATTERNS = {
    "ujjivan": [
        {
            "date": "Date",
            "transaction_note": "Particular",
            "debit": "Withdrawal",
            "credit": "Deposit",
            "balance": "Balance Amount",
            "chq_num": "Chq./Ref.no."
        }
    ]
}

SKIP_UNICODE_REMOVAL_LIST = ["ncb", "alrajhi", "mahagrambnk", "spcb", "tbc_bnk_georgia", "bnk_of_georgia"]

REGEXES_TO_SANITIZE_TRXN_COLUMN = {
    "veershaiv_bnk": {
        "date": ["(?i)txn\\s+date\\s+([0-9]{2}\\-[a-z]{3}\\-[0-9]{4})"],
        "debit": ["(?i)debit\\s+([0-9]{1,3}(?:\\,[0-9]{2,3})*\\.[0-9]{2})"],
        "credit": ["(?i)credit\\s+([0-9]{1,3}(?:\\,[0-9]{2,3})*\\.[0-9]{2})"],
        "balance": ["(?i)balance\\s+([0-9]{1,3}(?:\\,[0-9]{2,3})*\\.[0-9]{2}(?:Dr|Cr))"]
    },
    "tjsb_sahakari": {
        "balance": ["(?i)([0-9]{1,3}(?:\\,[0-9]{2,3})*\\.[0-9]{2})\\s+(?:Dr|Cr)"]
    }
}

EMAIL_MINIMUM_PERMISSIBLE_LENGTH = 1
INDIAN_PAN_NUMBER_MINIMUM_PERMISSIBLE_LENGTH = 10
INDIAN_PHONE_NUMBER_MINIMUM_PERMISSIBLE_LENGTH = 10

DATE_DELTA_DAYS_THRESHOLD_FOR_DATE_CORRECTION = 5

OPTIMIZATION_THRESHOLDS = {"sbi": {"ratio": 0.4, "count": 4}}

MAXIMUM_UPI_DAILY_LIMIT = 500000
MAXIMUM_UPI_PER_TRANSACTION_LIMIT = 500000
MAXIMUM_IMPS_DAILY_LIMIT = 500000
MAXIMUM_IMPS_PER_TRANSACTION_LIMIT = 500000
IMPS_RECOGNISATION_REGEX_LIST = ["(?i)^(?!.*(?:RTGS|NEFT).*IMPS.*)(?:\\s*IMPS|.*[\\/\\-\\*\\: ]+IMPS[\\/\\-\\*\\: ]+).*"]

SPLIT_TRANSACTION_NOTES_PATTERNS = {
    "hdfc": {"regex": r"(?i)(?:\s*statement)?\s*from\s*:\s*(\d{2}/\d{2}/\d{4})\s*to\s*:\s*(\d{2}/\d{2}/\d{4})", "keys": ["date", "transaction_note"]},
    "mizoram": {"regex": r"(?i)\s*brought\s*forward\:\s*[0-9\.]+", "keys": ["debit", "credit", "balance"]},
}