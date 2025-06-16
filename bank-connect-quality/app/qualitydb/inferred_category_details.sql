CREATE TABLE inferred_categories(
    id SERIAL PRIMARY KEY,
    category    VARCHAR(255) not null,
    category_description    TEXT,
    added_by    VARCHAR(255),
    is_active   BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO inferred_categories (category, category_description, added_by) VALUES ('Loan', 'Loan Transactions - Transaction Type is mostly Debit (credit is seen in some cases in IDBI bank)', 'admin');
INSERT INTO inferred_categories (category, category_description, added_by) VALUES ('Bank Charges', 'Bank Charges - Transaction Type is debit', 'admin');
INSERT INTO inferred_categories (category, category_description, added_by) VALUES ('Cash Withdrawal', 'Cash Withdrawal - Transaction Type is Debit', 'admin');
INSERT INTO inferred_categories (category, category_description, added_by) VALUES ('Cash Deposit', 'Cash Deposit - Transaction Type is Credit', 'admin');
INSERT INTO inferred_categories (category, category_description, added_by) VALUES ('Utilities', 'Utility Transactions - Transaction Type is Debit. (Generally payment made towards Recharge etc.)', 'admin');
INSERT INTO inferred_categories (category, category_description, added_by) VALUES ('Reversal', 'Reversal Transactions - Can be Credit/Debit.', 'admin');
INSERT INTO inferred_categories (category, category_description, added_by) VALUES ('Fixed Deposit', 'Can be Credit/Debit. Credit in cases of maturity, auto sweep in, maturity etc. Debit in cases of Sweepout, payment.', 'admin');
INSERT INTO inferred_categories (category, category_description, added_by) VALUES ('Travel', 'Debit Transaction. Payment towards Travel Providers', 'admin');
INSERT INTO inferred_categories (category, category_description, added_by) VALUES ('Loan Disbursed', 'Credit Transaction. Mostly Means - Receiving the loan amount upon disbursal', 'admin');
INSERT INTO inferred_categories (category, category_description, added_by) VALUES ('Interest', 'Credit Transaction. Means Interest Received from Savings bank accounts etc.', 'admin');
INSERT INTO inferred_categories (category, category_description, added_by) VALUES ('Interest Charges', 'Debit Transaction. Collected Due to Low Bank Account Balance etc.', 'admin');
INSERT INTO inferred_categories (category, category_description, added_by) VALUES ('Insurance', 'Can be Credit/Debit. Debit when paying towards schemes, Credit when receiving payments.', 'admin');
INSERT INTO inferred_categories (category, category_description, added_by) VALUES ('Online Shopping', 'Debit Transaction. Made towards online shopping platforms', 'admin');
INSERT INTO inferred_categories (category, category_description, added_by) VALUES ('EMI Payment', 'Debit Transaction. EMI payments towards Loan', 'admin');
INSERT INTO inferred_categories (category, category_description, added_by) VALUES ('Small Savings', 'Can be Credit/Debit. Credit when Getting money through govt schemes etc. Debit when contribution towards these yojanas or schemes', 'admin');
INSERT INTO inferred_categories (category, category_description, added_by) VALUES ('Fuel', 'Debit Transaction. Made towards Fuel Vendors.', 'admin');
INSERT INTO inferred_categories (category, category_description, added_by) VALUES ('Credit Card Payment', 'Debit Transaction. CC Payment.', 'admin');
INSERT INTO inferred_categories (category, category_description, added_by) VALUES ('Investment Expense', 'Can be Credit/Debit. Debit when paying towards investment sources like groww etc. Credit seen only in Equitas.', 'admin');
INSERT INTO inferred_categories (category, category_description, added_by) VALUES ('Personal Loan', 'Debit Transaction. Unsecured personal loans.', 'admin');
INSERT INTO inferred_categories (category, category_description, added_by) VALUES ('Purchase by Card', 'Debit Transaction. POS transactions.', 'admin');
INSERT INTO inferred_categories (category, category_description, added_by) VALUES ('Cash Back', 'Credit Transaction. Cashback from diff sources UPI etc. googlepay very prominently', 'admin');
INSERT INTO inferred_categories (category, category_description, added_by) VALUES ('Home Loan', 'Debit Transaction. Made towards House Loans.', 'admin');
INSERT INTO inferred_categories (category, category_description, added_by) VALUES ('Food', 'Debit Transaction. Made towards Food Platforms - Swiggy/Zomato/UberEats etc.', 'admin');
INSERT INTO inferred_categories (category, category_description, added_by) VALUES ('Share Purchase', 'Debit Transaction. Share Purchases mostly towards AngelBroking', 'admin');
INSERT INTO inferred_categories (category, category_description, added_by) VALUES ('Dividend', 'Credit Transaction. Dividend for holdings.', 'admin');
INSERT INTO inferred_categories (category, category_description, added_by) VALUES ('Bounced I/W Cheque Charges', 'Debit Transaction. Cheque Return Charges levied.', 'admin');
INSERT INTO inferred_categories (category, category_description, added_by) VALUES ('Salary', 'Credit Transaction. Salary.', 'admin');
INSERT INTO inferred_categories (category, category_description, added_by) VALUES ('Salary Paid', 'Debit Transaction. Mostly UPI Transaction Salary paid to people. Generally Salary Paid to employees.', 'admin');
INSERT INTO inferred_categories (category, category_description, added_by) VALUES ('Tax', 'Credit/Debit. Credit mostly in case of GST Refunds, Debit for tax paid for gst etc.', 'admin');
INSERT INTO inferred_categories (category, category_description, added_by) VALUES ('Bounced O/W Cheque', 'Debit Transaction. Insufficient Funds mostly cheque is rejected', 'admin');
INSERT INTO inferred_categories (category, category_description, added_by) VALUES ('Below Min Balance', 'Debit Transaction. Subset of Charges - mostly when Non Maintenance Charge is levied.', 'admin');
INSERT INTO inferred_categories (category, category_description, added_by) VALUES ('Bounced I/W Cheque', 'Credit Transaction. Transaction for bounced cheque.', 'admin');
INSERT INTO inferred_categories (category, category_description, added_by) VALUES ('Subsidy', 'Credit Transaction. Subsidies offered - generally lpg etc.', 'admin');
INSERT INTO inferred_categories (category, category_description, added_by) VALUES ('MF Purchase', 'Debit Transaction. Made towards mutual fund purchases.', 'admin');
INSERT INTO inferred_categories (category, category_description, added_by) VALUES ('Home Loan Disbursed', 'Credit Transaction. Like Loan Disbursed - but towards Home Loan.', 'admin');
INSERT INTO inferred_categories (category, category_description, added_by) VALUES ('Provident Fund Withdrawal', 'Credit Transaction. Withdrawn from PF.', 'admin');

INSERT INTO inferred_categories (category, category_description, added_by) VALUES ('Bounced I/W ECS Charges', 'Debit Transaction. Occurs when charge transaction has been happened for a bounce transaction.', 'admin');
INSERT INTO inferred_categories (category, category_description, added_by) VALUES ('Card Settlement', 'Credit Transaction. Occurs Only for Axis Bank. Means - Card Settlement from Merchant.', 'admin');
INSERT INTO inferred_categories (category, category_description, added_by) VALUES ('Entertainment', 'Debit Transaction. Occurs when made towards Platforms like Hotstar, JIO, Netflix etc.', 'admin');
INSERT INTO inferred_categories (category, category_description, added_by) VALUES ('Bounced I/W Payment', 'Credit Transaction. Return Transactions looks like this happens during Internet Banking Transfers.', 'admin');
INSERT INTO inferred_categories (category, category_description, added_by) VALUES ('Business Loan Disbursed', 'Credit Transaction. Like Loan Disbursed - but towards Business Loan.', 'admin');
INSERT INTO inferred_categories (category, category_description, added_by) VALUES ('Auto Loan', 'Debit Transaction. Loan Transactions related to purchase of automobiles', 'admin');
INSERT INTO inferred_categories (category, category_description, added_by) VALUES ('Bounced I/W ECS', 'Credit Transaction. Much similar to Auto Debit payment Bounce.', 'admin');
INSERT INTO inferred_categories (category, category_description, added_by) VALUES ('Loan Repayment', 'Debit Transaction. Loan Repayment explicitly written in Transaction Note.', 'admin');
INSERT INTO inferred_categories (category, category_description, added_by) VALUES ('Bounced O/W Payment', 'Debit Transaction. Payment bounce.', 'admin');
INSERT INTO inferred_categories (category, category_description, added_by) VALUES ('Penal Charges', 'Debit Transaction. Penalty Charges.', 'admin');
INSERT INTO inferred_categories (category, category_description, added_by) VALUES ('Share Sell', 'Credit Transaction. Credit from Security Companies upon selling shares.', 'admin');
INSERT INTO inferred_categories (category, category_description, added_by) VALUES ('Inward FX Remittance', 'Credit Transaction. Transfer to Indian Account from Foreign Account.', 'admin');
INSERT INTO inferred_categories (category, category_description, added_by) VALUES ('Investment Income', 'Credit Transaction. Earnings from Investment - Zerodha etc.', 'admin');