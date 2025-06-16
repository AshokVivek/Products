# TODO confirm what sweep trf is (it should be FD/TD to bank balance)

axis_debit = {'list_of_international_transaction_arbitrage': ['.*(RATE DIFF).*'],
              'list_of_bank_charge': ['.*(% ON CHARGE).*', '.*(CONSOLIDATED CHARGES).*', '.*(CARD CHARGES).*',
                                      '.*(INT.COLL).*'],
              'list_of_debit_card': ['^(POS).*'],
              'list_of_cash_withdrawl': ['.*(ATM-CASH).*', '.*(CASH WITHDRAWAL).*'],
              'list_of_net_banking_transfer': ['.*(IMPS).*', '.*(NEFT).*', '.*(RTGS).*', '^(MOB).*', '^(INB).*'],
              'list_of_bill_payment': ['.*(BILLDESK).*'],
              'list_of_auto_debit_payment': [],
              'list_of_payment_gateway_purchase': ['^(ECOM PUR).*'],
              'list_of_upi': ['.*(UPI).*'],
              'list_of_outward_cheque_bounce': [],
              'list_of_chq': ['.*(CLG-CHQ).*']
              }

icici_debit = {'list_of_international_transaction_arbitrage': ['.*(RATE DIFF).*', '.*(INT.PD).*'],
               'list_of_bank_charge': ['.*(SMSCHGS).*', '.*(DCARDFEE).*'],
               'list_of_debit_card': ['^(VPS).*', '^(IPS).*'],
               'list_of_cash_withdrawl': ['.*(CASH WDL).*', '^(VAT).*', '^(MAT).*', '^(NFS).*', '.*(CASH PAID:SELF).*'],
               'list_of_net_banking_transfer': ['.*(IMPS).*', '.*(NEFT).*', '.*(RTGS).*', '^(VIN).*', '^(IIN).*'],
               'list_of_bill_payment': ['.*(BILLDESK).*', '^(BIL).*'],
               'list_of_auto_debit_payment': ['^(LAGUR).*', '^(VSI).*'],
               'list_of_payment_gateway_purchase': ['^(RPI).*'],
               'list_of_upi': ['.*(UPI).*'],
               'list_of_outward_cheque_bounce': [],
               'list_of_chq': ['^(CLG).*'],
               'list_of_investment': ['^(To RD A/C).*']
               }

kotak_debit = {'list_of_international_transaction_arbitrage': ['.*(RATE DIFF).*'],
               'list_of_bank_charge': ['.*(CHRG).*', '.*(INT.COLL).*'],
               'list_of_debit_card': ['.*(PCD).*'],
               'list_of_cash_withdrawl': ['.*(ATW).*', '.*(ATL/).*', '.*(CASH WITHDRAWAL).*'],
               'list_of_net_banking_transfer': ['^(MB).*', '^(IB).*', '.*(IMPS).*', '.*(NEFT).*', '.*(RTGS).*'],
               'list_of_bill_payment': ['.*(DUES DEBITED).*', '.*(PAID CARD).*', '.*(TRF TO CARD).*', '.*(BILLPAY).*',
                                        '.*(BILLDESK).*'],
               'list_of_auto_debit_payment': ['.*(ECSIDR).*'],
               'list_of_payment_gateway_purchase': ['^(OS ).*'],
               'list_of_upi': ['.*(UPI).*'],
               'list_of_outward_cheque_bounce': ['.*(O/W RTN).*'],
               'list_of_chq': ['.*(CHEQUE).*', '.*(CHQ).*'],
               'list_of_investment': ['^(SWEEP TRF).*']
               }

sbi_debit = {'list_of_international_transaction_arbitrage': ['.*(RATE DIFF).*'],
             'list_of_bank_charge': ['.*(COMMISSION OF IMPS).*', '.*(MONTHLY AVE).*', '.*(ATM ANNUAL FEE).*',
                                     '.*(INSUF BAL).*', '.*(SMS CHARGES).*', '.*(CHARGES FOR SMS).*',
                                     '.*(DEBIT INTEREST).*', '.*(SERVICE CHARGES).*', '.*(FEE EXCESS).*',
                                     '.*(CHEQUE RETURNED CHARGES).*', '.*(CASH HANDLING CHARGES).*',
                                     '.*(\s*CHARGES).*'],
             'list_of_debit_card': ['.*(DEBIT CARD).*'],
             'list_of_cash_withdrawl': ['.*(ATM-CASH).*', '.*(ATM WDL).*', '.*(CASH WITHDRAWAL).*'],
             'list_of_net_banking_transfer': ['.*(IMPS).*', '.*(NEFT).*', '.*(RTGS).*', '.*(-\s*INB).*'],
             'list_of_bill_payment': ['.*(BILLDESK).*', '.*(CREDIT CARD).*'],
             'list_of_auto_debit_payment': ['.*(ACHDR).*', '.*(DDR).*'],
             'list_of_payment_gateway_purchase': [],
             'list_of_upi': ['.*(UPI).*'],
             'list_of_outward_cheque_bounce': ['.*(OUT-CHQ RETURN).*'],
             'list_of_chq': ['.*(CHQ TRANSFER).*', '.*(TO CLEARING).*'],

             }

hdfc_debit = {'list_of_international_transaction_arbitrage': ['.*(DC INTL).*', '.*(RATE DIFF).*'],
              'list_of_bank_charge': [],
              'list_of_debit_card': ['^(POS).*'],
              'list_of_cash_withdrawl': ['.*(NWD).*', '.*(EAW).*', '.*(ATW).*'],
              'list_of_net_banking_transfer': ['.*(IMPS).*', '.*(NEFT).*', '.*(RTGS).*'],
              'list_of_bill_payment': ['.*(BILLDESK).*', '.*(CREDIT CARD).*', '.*(BILLPAY).*'],
              'list_of_auto_debit_payment': ['^(ECS).*'],
              'list_of_payment_gateway_purchase': [],
              'list_of_upi': ['.*(UPI).*'],
              'list_of_outward_cheque_bounce': [],
              'list_of_chq': ['.*(CHQ PAID).*']
              }

generic_debit = {
    'list_of_international_transaction_arbitrage': ['RATE.DIFF'],
    'list_of_net_banking_transfer': ['.*(IMPS).*', '.*(NEFT).*', '.*(RTGS).*'],
    'list_of_cash_withdrawl': ['.*(ATM-CASH).*', '.*(ATM WDL).*', '.*(CASH WITHDRAWAL).*'],
    'list_of_bill_payment': ['.*(BILLPAY).*'],

}

axis_debit_priority_order = [
    'list_of_international_transaction_arbitrage',
    'list_of_bill_payment',
    'list_of_cash_withdrawl',
    'list_of_bank_charge',
    'list_of_debit_card',
    'list_of_outward_cheque_bounce',
    'list_of_chq',
    'list_of_upi',
    'list_of_auto_debit_payment',
    'list_of_net_banking_transfer',
    'list_of_payment_gateway_purchase'
]

kotak_debit_priority_order = [
    'list_of_international_transaction_arbitrage',
    'list_of_bill_payment',
    'list_of_cash_withdrawl',
    'list_of_bank_charge',
    'list_of_debit_card',
    'list_of_outward_cheque_bounce',
    'list_of_chq',
    'list_of_upi',
    'list_of_auto_debit_payment',
    'list_of_net_banking_transfer',
    'list_of_payment_gateway_purchase'
]

sbi_debit_priority_order = [
    'list_of_international_transaction_arbitrage',
    'list_of_bill_payment',
    'list_of_cash_withdrawl',
    'list_of_bank_charge',
    'list_of_debit_card',
    'list_of_outward_cheque_bounce',
    'list_of_chq',
    'list_of_upi',
    'list_of_auto_debit_payment',
    'list_of_net_banking_transfer',
    'list_of_payment_gateway_purchase'
]

hdfc_debit_priority_order = [
    'list_of_international_transaction_arbitrage',
    'list_of_bill_payment',
    'list_of_cash_withdrawl',
    'list_of_bank_charge',
    'list_of_debit_card',
    'list_of_outward_cheque_bounce',
    'list_of_chq',
    'list_of_upi',
    'list_of_auto_debit_payment',
    'list_of_net_banking_transfer',
    'list_of_payment_gateway_purchase'
]

icici_debit_priority_order = [
    'list_of_international_transaction_arbitrage',
    'list_of_bill_payment',
    'list_of_cash_withdrawl',
    'list_of_bank_charge',
    'list_of_debit_card',
    'list_of_outward_cheque_bounce',
    'list_of_chq',
    'list_of_upi',
    'list_of_auto_debit_payment',
    'list_of_net_banking_transfer',
    'list_of_payment_gateway_purchase'
]


def get_debit_dict(bank):
    priority_order = []
    channel_dict = {}

    if bank == 'axis':
        channel_dict = axis_debit
        priority_order = axis_debit_priority_order
    elif bank == 'kotak':
        channel_dict = kotak_debit
        priority_order = kotak_debit_priority_order
    elif bank == 'sbi':
        channel_dict = sbi_debit
        priority_order = sbi_debit_priority_order
    elif bank == 'hdfc':
        channel_dict = hdfc_debit
        priority_order = hdfc_debit_priority_order
    elif bank == 'icici':
        channel_dict = icici_debit
        priority_order = icici_debit_priority_order

    return channel_dict, priority_order
