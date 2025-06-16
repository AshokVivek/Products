import re

emi_patt2 = re.compile(
    "^(?:.*[\\/\\-\\+\\_\\,\\@\\. ]+|\\s*)(EMI)(?:[\\/\\-\\+\\_\\,\\@\\. ]+.*|\\s*)$",
    flags=re.IGNORECASE,
)


def categorise_lender_transaction_single_category(transaction):
    merchant_category = transaction["merchant_category"].strip()
    transaction_type = transaction["transaction_type"]
    transaction_note = transaction["transaction_note"]
    category = ""
    if merchant_category == "loans":
        category = "Loan"
    elif merchant_category == "home_loan":
        category = "Home Loan"
    elif merchant_category == "gold_loan":
        category = "Gold Loan"
    elif merchant_category == "auto_loan":
        category = "Auto Loan"
    elif merchant_category == "personal_loan":
        category = "Personal Loan"
    elif merchant_category == "business_loan":
        category = "Business Loan"
    elif merchant_category == "term_loan":
        category = "Term Loan"

    if transaction_type == "credit":
        category = category + " Disbursed"
    elif transaction_type == "debit" and emi_patt2.match(transaction_note):
        category = "EMI Payment"
    transaction["category"] = category
    return transaction


def categorise_mutual_funds(transaction):
    merchant_category = transaction["merchant_category"]
    description = transaction["description"]
    transaction_type = transaction["transaction_type"]
    mutual_funds_list = "mutual_funds"
    if merchant_category in mutual_funds_list or description in mutual_funds_list:
        if transaction_type == "debit":
            transaction["category"] = "MF Purchase"
        else:
            transaction["category"] = "MF Redemption"
    return transaction


categories_data = [
    {
        "category": "Cash Withdrawal",
        "transaction_channel": "cash_withdrawl",
        "description": "",
        "merchant_category": "",
    },
    {
        "category": "Bank Charges",
        "transaction_channel": "bank_charge",
        "description": "",
        "merchant_category": "",
    },
    {
        "category": "Bank Charges",
        "transaction_channel": "international_transaction_arbitrage",
        "description": "",
        "merchant_category": "",
    },
    {
        "category": "Utilities",
        "transaction_channel": "",
        "description": "",
        "merchant_category": "bills",
    },
    {
        "category": "Utilities",
        "transaction_channel": "",
        "description": "",
        "merchant_category": "utilities",
    },
    {
        "category": "Utilities",
        "transaction_channel": "",
        "description": "telco_bill",
        "merchant_category": "",
    },
    {
        "category": "Utilities",
        "transaction_channel": "",
        "description": "electric_bill",
        "merchant_category": "",
    },
    {
        "category": "Loan",
        "transaction_channel": "",
        "description": "lender_transaction",
        "merchant_category": "loans",
    },
    {
        "category": "Home Loan",
        "transaction_channel": "",
        "description": "lender_transaction",
        "merchant_category": "home_loan",
    },
    {
        "category": "Personal Loan",
        "transaction_channel": "",
        "description": "lender_transaction",
        "merchant_category": "personal_loan",
    },
    {
        "category": "Loan Repayment",
        "transaction_channel": "",
        "description": "",
        "merchant_category": "",
    },
    {
        "category": "Cash Deposit",
        "transaction_channel": "cash_deposit",
        "description": "",
        "merchant_category": "",
    },
    {
        "category": "Credit Card Payment",
        "transaction_channel": "",
        "description": "credit_card_bill",
        "merchant_category": "",
    },
    {
        "category": "Refund",
        "transaction_channel": "refund",
        "description": "",
        "merchant_category": "",
    },
    {
        "category": "Reversal",
        "transaction_channel": "reversal",
        "description": "",
        "merchant_category": "",
    },
    {
        "category": "Fuel",
        "transaction_channel": "",
        "description": "",
        "merchant_category": "fuel",
    },
    {
        "category": "Interest",
        "transaction_channel": "bank_interest",
        "description": "",
        "merchant_category": "",
    },
    {
        "category": "Tax",
        "transaction_channel": "",
        "description": "",
        "merchant_category": "tax",
    },
    {
        "category": "Tax",
        "transaction_channel": "",
        "description": "gst",
        "merchant_category": "",
    },
    {
        "category": "Travel",
        "transaction_channel": "",
        "description": "",
        "merchant_category": "travel",
    },
    {
        "category": "Card Settlement",
        "transaction_channel": "",
        "description": "card_settlement",
        "merchant_category": "",
    },
    {
        "category": "Cash Back",
        "transaction_channel": "",
        "description": "cash_back",
        "merchant_category": "",
    },
    {
        "category": "Bounced I/W ECS Charges",
        "transaction_channel": "bank_charge",
        "description": "ach_bounce_charge",
        "merchant_category": "",
    },
    {
        "category": "Investment Expense",
        "transaction_channel": "",
        "description": "investments",
        "merchant_category": "investments",
    },
    {
        "category": "Investment Expense",
        "transaction_channel": "",
        "description": "trading/investments",
        "merchant_category": "trading/investments",
    },
    {
        "category": "Fixed Deposit",
        "transaction_channel": "",
        "description": "fixed_deposit",
        "merchant_category": "",
    },
    {
        "category": "Insurance",
        "transaction_channel": "",
        "description": "insurance",
        "merchant_category": "insurance",
    },
    {
        "category": "Purchase by Card",
        "transaction_channel": "debit_card",
        "description": "",
        "merchant_category": "",
    },
    {
        "category": "Salary Paid",
        "transaction_channel": "salary_paid",
        "description": "",
        "merchant_category": "",
    },
    {
        "category": "Online Shopping",
        "transaction_channel": "",
        "description": "",
        "merchant_category": "online_shopping",
    },
    {
        "category": "Shopping",
        "transaction_channel": "",
        "description": "",
        "merchant_category": "shopping",
    },
    {
        "category": "Bounced I/W Cheque",
        "transaction_channel": "inward_cheque_bounce",
        "description": "",
        "merchant_category": "",
    },
    {
        "category": "Bounced O/W Cheque",
        "transaction_channel": "outward_cheque_bounce",
        "description": "",
        "merchant_category": "",
    },
    {
        "category": "Below Min Balance",
        "transaction_channel": "bank_charge",
        "description": "min_bal_charge",
        "merchant_category": "",
    },
    {
        "category": "Food",
        "transaction_channel": "",
        "description": "",
        "merchant_category": "food",
    },
    {
        "category": "Subsidy",
        "transaction_channel": "",
        "description": "subsidy",
        "merchant_category": "",
    },
    {
        "category": "MF Purchase",
        "transaction_channel": "",
        "description": "mutual_funds",
        "merchant_category": "mutual_funds",
    },
    {
        "category": "Auto Loan",
        "transaction_channel": "",
        "description": "lender_transaction",
        "merchant_category": "auto_loan",
    },
    {
        "category": "Interest",
        "transaction_channel": "bank_interest",
        "description": "",
        "merchant_category": "",
    },
    {
        "category": "Interest Charges",
        "transaction_channel": "cc_interest",
        "description": "",
        "merchant_category": "",
    },
    {
        "category": "Small Savings",
        "transaction_channel": "",
        "description": "small_savings",
        "merchant_category": "",
    },
    {
        "category": "Salary",
        "transaction_channel": "salary",
        "description": "",
        "merchant_category": "",
    },
    {
        "category": "UPI Settlement",
        "transaction_channel": "",
        "description": "upi_settlement",
        "merchant_category": "",
    },
    {
        "category": "Bounced I/W ECS",
        "transaction_channel": "auto_debit_payment_bounce",
        "description": "`",
        "merchant_category": "",
    },
    {
        "category": "Bounced I/W Cheque Charges",
        "transaction_channel": "bank_charge",
        "description": "chq_bounce_charge",
        "merchant_category": "",
    },
    {
        "category": "Dividend",
        "transaction_channel": "",
        "description": "dividend",
        "merchant_category": "",
    },
    {
        "category": "Share Purchase",
        "transaction_channel": "",
        "description": "share_purchase",
        "merchant_category": "",
    },
    {
        "category": "MF Redemption",
        "transaction_channel": "",
        "description": "mutual_funds",
        "merchant_category": "mutual_funds",
    },
    {
        "category": "Share Sell",
        "transaction_channel": "",
        "description": "share_sell",
        "merchant_category": "",
    },
    {
        "category": "Brokerage",
        "transaction_channel": "",
        "description": "brokerage",
        "merchant_category": "",
    },
    {
        "category": "Bounced I/W Payment",
        "transaction_channel": "inward_payment_bounce",
        "description": "",
        "merchant_category": "",
    },
    {
        "category": "Bounced I/W Payment Charges",
        "transaction_channel": "",
        "description": "",
        "merchant_category": "",
    },
    {
        "category": "Bounced O/W Payment",
        "transaction_channel": "",
        "description": "",
        "merchant_category": "",
    },
    {
        "category": "Bounced O/W Payment Charges",
        "transaction_channel": "",
        "description": "",
        "merchant_category": "",
    },
    {
        "category": "Gaming",
        "transaction_channel": "",
        "description": "",
        "merchant_category": "gambling",
    },
    {
        "category": "Entertainment",
        "transaction_channel": "",
        "description": "",
        "merchant_category": "entertainment",
    },
    {
        "category": "Provident Fund Withdrawal",
        "transaction_channel": "",
        "description": "",
        "merchant_category": "",
    },
    {
        "category": "Pension",
        "transaction_channel": "",
        "description": "pension",
        "merchant_category": "",
    },
    {
        "category": "Penal Charges",
        "transaction_channel": "bank_charge",
        "description": "penalty_charge",
        "merchant_category": "",
    },
    {
        "category": "Bounced O/W ECS",
        "transaction_channel": "",
        "description": "",
        "merchant_category": "",
    },
    {
        "category": "Bounced O/W Cheque Charges",
        "transaction_channel": "bank_charge",
        "description": "chq_bounce_charge",
        "merchant_category": "",
    },
    {
        "category": "Website Charges",
        "transaction_channel": "",
        "description": "",
        "merchant_category": "",
    },
    {
        "category": "Advance Salary Paid",
        "transaction_channel": "salary_paid",
        "description": "",
        "merchant_category": "",
    },
    {
        "category": "Provident Fund Contribution",
        "transaction_channel": "",
        "description": "",
        "merchant_category": "",
    },
    {
        "category": "Inward FX Remittance",
        "transaction_channel": "",
        "description": "",
        "merchant_category": "",
    },
    {
        "category": "Charity",
        "transaction_channel": "",
        "description": "",
        "merchant_category": "",
    },
    {
        "category": "Reimbursement",
        "transaction_channel": "",
        "description": "",
        "merchant_category": "",
    },
    {
        "category": "Gold Loan",
        "transaction_channel": "",
        "description": "lender_transaction",
        "merchant_category": "gold_loan",
    },
    {
        "category": "Clothing",
        "transaction_channel": "",
        "description": "",
        "merchant_category": "shopping",
    },
    {
        "category": "Stop Payment",
        "transaction_channel": "",
        "description": "",
        "merchant_category": "",
    },
    {
        "category": "House Rent",
        "transaction_channel": "",
        "description": "",
        "merchant_category": "",
    },
    {
        "category": "Medical",
        "transaction_channel": "",
        "description": "",
        "merchant_category": "medical",
    },
    {
        "category": "Foreign Currency Expense",
        "transaction_channel": "",
        "description": "",
        "merchant_category": "",
    },
    {
        "category": "Crypto Transaction",
        "transaction_channel": "",
        "description": "",
        "merchant_category": "",
    },
    {
        "category": "Advance Salary",
        "transaction_channel": "salary",
        "description": "",
        "merchant_category": "",
    },
    {
        "category": "Term Loan",
        "transaction_channel": "",
        "description": "lender_transaction",
        "merchant_category": "term_loan",
    },
    {
        "category": "Loan Payment",
        "transaction_channel": "",
        "description": "lender_transaction",
        "merchant_category": "loans",
    },
    {
        "category": "Bounced O/W ECS Charges",
        "transaction_channel": "",
        "description": "ach_bounce_charge",
        "merchant_category": "",
    },
    {
        "category": "Self Transfer",
        "transaction_channel": "",
        "description": "self_transfer",
        "merchant_category": "",
    },
    {
        "category": "Business Loan",
        "transaction_channel": "",
        "description": "lender_transaction",
        "merchant_category": "business_loan",
    },
    {
        "category": "Alcohol",
        "transaction_channel": "",
        "description": "",
        "merchant_category": "alchohol",
    },
    {
        "category": "Rent Received",
        "transaction_channel": "",
        "description": "rent_received",
        "merchant_category": "",
    }
]
