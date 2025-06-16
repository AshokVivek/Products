# library (previously fsmlib)

This is part of the archived repository `fsmlib` which can be found [here](https://github.com/finbox-in/fsmlib).

-- The repository is merged as an effort to further smoothen development efforts across services. For git-blame please refer to the link above to find past commits & other historical information.

fsmlib is a library of functions used for processing bank transactional statements. The library contains functions for the following:

- Extract transactional details from transactional statement (PDF) of more than 20 major banks serving in India.
- Extract features like recurring transactions, account-holder salary for a given statement.
- Generate a comprehensive report on the transaction history of the user.

It is used as a git submodule and runs on a **Python 3.8** environment.

## Running locally

- First set up a local environment as follows:

```sh
git clone https://github.com/finbox-in/fsmlib
cd fsmlib
```

```sh
python3 -m venv .venv
```

- Activate environment:

```sh
source .venv/bin/activate
```

pip: (requires git installed)

```sh
pip install git+https://github.com/pdftables/python-pdftables-api.git
```

pip: (without git)

```sh
pip install https://github.com/pdftables/python-pdftables-api/archive/master.tar.gz
```

After installing the above dependencies run:

```sh
pip install -r requirements.txt
```

> use `deactivate` to exit current environment

## Testing Functions

- Import testing function from testring.py `from library.testing_rig import *`

### test_transactions(['pdf_path'], bank_name, password, page_num)

- Returns all extracted transaction form pdf's specified page number.
- default page_num is 0

### test_identity(['pdf_path'], bank_name)

- Returns identity for the the given pdf.
- `{'identity': {'account_number': '26020200000372', 'name': 'DR. R.M.L HOSPITAL, NEW DELHI', 'address': 'Account Number : 26020200000372 Name : SM SKILLS MANPOWER SOLUTIONS Currency Code : INR Branch Name : DR. R.M.L HOSPITAL, NEW DELHI', 'ifsc': None, 'micr': None, 'account_category': None}, 'keywords': {'amount_present': True, 'balance_present': True, 'date_present': True, 'all_present': True}, 'date_range': {'from_date': '2020-11-16', 'to_date': '2020-12-07'}, 'is_fraud': False, 'fraud_type': None}`
- account_number is must, if account_number is none then pdf is not parsable

### get_transaction_channel(df, bank)

- Return transaction channel for given dataframe
- Run different regex from `transaction_channel_credit_dict.py / transaction_channel_debit_dict.py` on transaction note to get channel.
- Different transaction channel for credit / debit `'international_transaction_arbitrage','bank_interest','cash_deposit','net_banking_transfer','auto_debit_payment_bounce','upi','chq','investment_cashin','salary','refund','inward_cheque_bounce', investment ,outward_cheque_bounce ,payment_gateway_purchase , auto_debit_payment, bank_charge`

### transaction_description(transaction_row)

- Get transaction_description for a given row.
- Serch for lender name in transaction note from    `lender_list.py`
- Update `is_lender` to `true` if lender name is present in transaction note

## Bank Template

- New template is added for Unparsed pdf's
- Take the bounding box for different texts on the pdf
- Basic template consist of `accnt_box` , `date_box`, `address_bbox`, `ifsc_bbox` and `trans_bbox`.
- Regex is used to filter the text from a given bounding box
- Helper function for fetching info is in `fitz_functions.py` 

### Sample Template Structure

```json
{
"accnt_bbox": [{
"bbox": [10,80,650,300],
"regex":  "(?i).*?Account\\s*(?:No|Number)\\s*:?\\s*([0-9]+).*"
},

"date_bbox": [{
"from_bbox": [10,10,500,300],
"from_regex":  "(?i).*?SpecifyPeriod\\s*:?(.*?)to.*",
"to_bbox": [10,10,500,300],
"to_regex":  "(?i).*?SpecifyPeriod.*?to:?([0-9]{1,2}[-|/|\\s*][0-9A-Z]"
},

"address_bbox": [{
"bbox": [6,135,582,239],
"regex":  "(?i)(.*?)Account\\s*Statement.*"
},

"ifsc_bbox": [{
"bbox": [15,62,300,200],
"regex":  "(?i).*?IFSC\\s*CODE\\s*\\:?\\s*([A-Z]{4}[0-9]{7}).*"
},

"trans_bbox": [{
"vertical_lines":  true,
"horizontal_lines":  true,
"column": [["random","date","transaction_note","debit","credit","balance"]]
}]
```

### Calculate bbox

- Bounding box will give us  4 values `top`, `width`,   `left`, `height`
- `x1 = left`
- `y1 =  top`
- `x2 = left + width`
- `y2 = top + height`

#### Calculate trans_bbox

#### vertical_lines

- `true` when line is visible on the pdf.
- `[x1,y1,x2,y2]` when vertical line are not visible.

#### horizontal_lines

- `true` when horizontal_lines are visible on template
- `text` when each transaction is separated

#### column

- Number of the column must be the same as in the pdf
- `date`, `transaction_note`, `debit/credit/amount`, `balance` are required to parse the pdf.
- use `random` for a column that is not required.

### Important Keys in Template Json

- `{merge:true}` In json means merger the transaction's DF w.r.t to space between different transactions.
- `{footer:[true,<num_of_footer>]}` If footer is set true, its will remove <num_of_footer> transaction's from the bottom on each page.


### How to assign uuids to templates using script
- NOTE: from the current folder 'fsmlib'
```shell
> ipython
> from template_utils_rig import *
> assign_template_uuids()
```