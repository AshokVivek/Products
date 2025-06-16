from .conf import QUALITY_SECRET, ALGORITHM, SLACK_TOKEN, SLACK_CHANNEL, redis_cli
from datetime import datetime, timedelta
from typing import Optional
import jwt, time, random, sentry_sdk, copy, os, pandas as pd, numpy as np
from passlib.context import CryptContext
from .dependencies import get_user
from app.database_utils import portal_db, prepare_clickhouse_client
from slack_sdk import WebClient
from app.database_utils import quality_database
from app.conf import RAMS_POST_PROCESSING_QUEUE_URL, sqs_client
import json
from uuid import uuid4

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
file_path = '/tmp'

ACCOUNT_CATEGORY_KEYWORDS=['type', 'giro', 'FISB', 'HSS', 'SBC', 'SBA', 'OLCC', 'GOLD', 'CHSB', 'CASH', 'schme', 'PMJDY', 'SBGEN', 'BSBDASBELT', 'SBCRP', 'SBCHQ', 'SBSAL', 'ALSA', 'SBTWO', 'SBCMP', 'CAGEN', 'SMART', 'SBWEL', 'CAELT', 'CAPRM', 'ODUNN', 'OD-CC', 'SBCLS', 'scheme', 'produk', 'saving', 'SALARY', 'SAVER', 'CDPUB', 'NORMAL', 'INDIE', 'SBDIQ', 'SAPPR', 'SB/GEN', 'CA-IND', 'CSP SB', 'CC-SME', 'product', 'current', 'SAVINGS', 'SBNCHQ', 'NORMAL', 'REGULAR', 'SHAKTI', 'SB_GEN', 'CLASSIC', 'FREEDOM', 'SB - FI', 'PRAGATI', 'Address', 'category', 'a\\c type', 'STD-MOD', 'BUSINESS', 'Sav-Chq', 'TDR-PUB', 'corporate', 'overdraft', 'UPI EASY', 'VISHESH', 'INDUS MAX', 'CD GENRAL', 'NRO PRIME', 'CACAP-DOM', 'individual', 'over draft', 'SB Regular', 'SB:SAVINGS', 'SB GENERAL', 'YUVASHAKTI', 'PROPRIETOR', 'SB-General', 'SB-DIGITAL', 'CA GENERAL', 'CR:CURRENT', 'INDUS BLUE', 'PMMY-TARUN', 'NRE NORMAL', 'SB PREMIUM', 'enterprise', 'description', 'Cash Credit', 'SB-ACCOUNT', 'DIGI-START', 'ENTERPRISE', 'SBB CGTMSE', 'EASYACCESS', 'aXcess Plus', 'MSME-OD/OCC', 'Savings A/c', 'PROPRIETORY', 'CURRENT A/C', 'SAVING BANK', 'CA - GLOBAL', 'SOD SyndMSE', 'HSS-GEN-PUB', 'SB DSP GOLD', 'IDFC Shakti', '(CA)GENERAL', 'BSBDA - 111', 'account type', 'STAFF SALARY', 'AXCESS PLUS', 'SB-PRIORITY', 'HONOUR FIRST', 'IDFC VISHESH', 'VISHESH ALSA', 'SB SGSP GOLD', 'customer type', 'VISHESH ALSA', 'INDUS DELITE', 'AXIS LIBERTY', 'IDFC VISHESH', 'SB CSP SILVER', 'PMJDY BSBD-OD', 'First Booster', 'CANARA GALAXY', 'INDUS COMFORT', 'CA-RURAL-FIRM', 'SBCHQ-GEN-IND', 'NEW CA CLASSIC', 'BUSINESS EDGE', 'DHANAM VANITHA', 'BANKER SALARY', 'FREEDOM FLEXI', 'PRIVILEGE MAX', 'CANARA GALAXY', 'BASIC BANKING', 'FIRST BOOSTER', 'SBSAL-DEFENCE', 'SB-EASYACCESS', 'INDUS COMFORT', 'Saving Account', 'CA-KVB-ECONOMY', 'CA FOR DE-LITE', 'SB-CHQ-IND-INR', 'RegularSavings', 'PROPRIETORSHIP', 'SAVING DEPOSIT', 'CA-GEN-IND-INR', 'EB-MSME-CC-SHG', 'OCC-MUDRA-REPO', 'GENERAL SAVING', 'SAVING BANK AC', 'UNION PROGRESS', 'CA-GEN-PUB-ALL', 'CGFT OVERDRAFT', 'COMFORT MAXIMA', 'MULTIPLIER MAX', 'Savings Account', 'Current Account', 'SAVING DEPOSITS', 'SAVING BANK A/C', 'Savings Regular', 'CUB SAVINGS A/C', 'IDFC Enterprise', 'CANARA SB STAFF', 'CUB SALARY PLUS', 'SBSAL-DEFENCE A', 'EB-CC-CLP-MUDRA', 'First Advantage', 'Current Deposit', 'HSS-GEN-PUB-IND', 'SB-IND-WITH CHQ', 'CUB YOUNG INDIA', 'SAVINGS DEPOSIT', 'CR CURRENT A/C.', 'SB-CHQ-IND-SEMI', 'Regular Savings', 'OVERDRAFT MUDRA', 'CANARA ELITE CA', 'account category', 'SENIOR PRIVILEG', 'SUPREME PAYROLL', 'FIRST SIGNATURE', 'COMFORT PREMIUM', 'CURRENT ACCOUNT-', 'CURRENT DEPOSITS', 'Cur-FI-CBC-Other', 'EB-MSME-CC-PMEGP', 'EASYACCESS-PRIME', 'CURRENT- GENERAL', 'EB-MSME-CC-e-DFS', 'YONO VKYC SB CHQ', 'BR-CC-Stocks-SBF', 'Freedom Flexi 45', 'UPI EASY ACCOUNT', 'Current Accounts', 'HSS-CENT SAMARTH', 'CA - CHANNEL ONE', 'SAVINGS ACCOUNT-', 'CANARA PRIVILEGE', 'CC - CASH CREDIT', 'PMJDY BSBD-OD-FI', 'SAVINGS DEPOSITS', 'PARTNERSHIP FIRM', 'SBB CGTMSE LIMIT', 'CC-Cent GST Loan', 'CA-GEN-PUB-METRO', 'G.H.B. PANDESARA', 'Business account', 'SAVINGS - GENERAL', 'CANARA SB GENERAL', 'IDFC PARAM (ALSA)', 'OVERDRAFT ACCOUNT', 'Freedom Flexi 100', 'CA Corporate -OCA', 'CA-SEMIURBAN-FIRM', 'Freedom Flexi 300', 'CA - FOR ARTHIYAS', 'SBCHQ-GEN-PUB-IND', 'Business Standard', 'Savings Regular', 'JIFFY ZERO BALANCE', 'PREMIUM PRIVELEGE', 'PRIVILEGE BANKING', 'GOVERNMENT SALARY', 'STAR SARAL BACHAT', 'SAVINGS-EASYACCESS', 'SB-CHQ-GEN-PUB-IND', 'SC Digital Account', 'SB-PMJDY BASIC KYC', 'RMGB TINY SB PMJDY', 'CUB SALARY SAVINGS', 'SB CSP CONT SILVER', 'Resi-Savings Staff', 'CASH CREDIT SCHEME', 'Online Savings 10K', 'Yes Family Primary', 'SB MINOR-RURAL-INR', 'Over Draft Account', 'YES PRAGATI VYAPAR', 'CA IND TRANSACTION', 'CA-GEN-PUB-ALL-INR', 'SB-DIGITAL-ACCOUNT', 'CA-GEN-PUB-IND-INR', 'MC-C C Stocks(SBF)', 'Individuals Saving', 'CD-CURRENT ACCOUNT', 'SAVINGS A/C SALARY', 'OVERDRAFT ACCOUNT-', 'SAVINGS BANK - GSSA', 'DIGITAL OVD SB- CHQ', 'Savings Regular 10k', 'SAVING MIN BAL ZERO', 'BARODA BACHAT MITRA', 'SAVING BANK DEPOSIT', 'SAVINGS BANK PUBLIC', 'Cash Credit Account', 'SOLE PROPRIETORSHIP', 'CHQ. SAVING DEPOSIT', 'SB-PRIORITY BANKING', 'SBCHQ-GEN-PUB-METRO', 'SAVING DEPOSIT-2201', 'HOME LOAN LINKED SB', 'Customisable CA-50K', 'GENERAL SAVING BANK', 'CASH CREDIT GENERAL', 'LOW INCOME SB (KYC)', 'CURRENT DEPOSIT A/C', 'Individuals Current', 'CR-IND. CURRENT A/C', 'CURRENT- (RURAL/SU)', 'SAVINGS ACCOUNT PRO', 'SB-NCHQ-GEN-PUB-IND', 'CA - BUSINESS VALUE', 'SAVINGS ACCOUNT-NON', 'BARODA BUSINESS C A', 'SAVINGS ACCOUNT 500', 'SBCHQ-RURAL-PUB-IND', 'SAVINGS ACCOUNT-RES', 'HSS--SALARY-IND-ALL', 'CA-GEN-SOC/BANK-INR', 'aXcess Plus - STAFF', 'statement of account', 'MERCHANT MULTIPLIER', 'SAVINGS BANK GENERAL', 'Core Savings Account', 'CURRENT DEPOSIT-2301', 'PRIME SALARY ACCOUNT', 'SS (SAVINGS ACCOUNT)', 'New Business Account', 'SAVINGS BANK ACCOUNT', 'Vishesh Staff Salary', 'SB-CHQ-IND-RURAL-INR', 'CA - BUSINESS SELECT', 'CD-GEN-PUB-IND-URBAN', 'SOD AGAINST PROPERTY', 'SAVINGS BANK-GENERAL', 'CURRENT ACCOUNT BFIL', 'SAVINGS BANK - STAFF', 'SB-CHQ-IND-STAFF-INR', 'CURRENT ACCOUNT-BFIL', 'EB-MSME-CC-SBF-MUDRA', 'CA-GEN-PUB-IND-RURAL', 'Current Account -10K', 'SB NONCHQ-GEN-PUB-SU', 'SBNCHQ-GEN-PUB-METRO', 'EB-MSME-CC-SSI-MUDRA', 'AXIS LIBERTY ACCOUNT', 'SB-CHQ-IND-URBAN-INR', 'CSBAdvantageSalarySA', 'CURRENT DEPOSIT(RES)', 'DIGITAL OVD SB- NCHQ', 'CA-GEN-PUB-RURAL-IND', 'SB-CHQ-RURAL-PUB-IND', 'Saving Bank Deposits', 'Savings Regular 10k', 'SAVINGS BANK GENERAL', 'Staff Salary-Standard', 'DIGITAL CA INDIVIDUAL', 'SAVINGS ACCOUNT-INDUS', 'CURRENT ACCOUNT-INDUS', 'SAVINGS BANK - SALARY', 'CURRENT DE-PUBLIC-ALL', 'Current Account - 25K', 'SBCHQ-GEN-PUB-IND-INR', 'GABBEEIA MALLICK PARA', 'BUSINESS EDGE ACCOUNT', 'SB-NCHQ-RURAL-PUB-IND', 'SB Sanchay - 2000 MAB', 'BARODA SALARY CLASSIC', 'CA - BUSINESS CLASSIC', 'HSS-GEN-PUB-OTH-RURAL', 'HSS-GEN-PUB-IND-METRO', 'SB-NCHQ-SEMIURBAN-IND', 'PRIME SAVINGS ACCOUNT', 'CURRENT DE PUBLIC ALL', 'Regular Savings - 121', 'SB-NCHQ-URBAN-PUB-IND', 'CURRENT ACCNT-GENERAL', 'Wings Savings Account', 'Business plus Account', 'Current Account - 50K', 'CURRENT ACCOUNT RFODA', 'CC-Mudra Kishore Loan', 'SAVING BANK ELITE A/C', 'SB-CHQ-IND-RURAL- INR', 'SAHAJ SAVINGS ACCOUNT', 'Regular Savings - 171', 'SBBASIC-PUB-IND-RURAL', 'Current Account Basic', 'HSS-GEN-PUB-IND-RURAL', 'CA Biz Stand 5000 MAB', 'OD Banks Deposits PER', 'SAVINGS-GENERAL-URBAN', 'RR-CC-SME CREDIT CARD', 'Other Current Deposit', 'HSS-GEN-PUB-IND-URBAN', 'KISAN SAVINGS ACCOUNT', 'CURRENT ACCOUNT CAENT', 'Wings Current Account', 'New Business Account', 'LIBERTY SALARY ACCOUNT', 'CURRENT ACCOUNT-NORMAL', 'Supreme Payroll Scheme', 'KRISHI SAVINGS ACCOUNT', 'BASIC SAVINGS BANK INR', 'SAVINGS BANK - REGULAR', 'CURRENT DEPOSIT - 2301', 'CA FOR STARTUP BANKING', 'Current Choice Account', 'SB-SalaryGain-Pub-MCLR', 'SAVING ACCOUNT REGULAR', 'EB-MSME-CC-SMART SCORE', 'SAVINGS COMFORT CHOICE', 'SB TINY SPL-OD-GEN-PUB', 'PRIVILEGE SAVINGS BANK', 'RURAL BUSINESS BANKING', 'CA-SHUBHARAMBH STARTUP', 'SBNCHQ-GEN-PUB-IND-INR', 'CURRENT ACCOUNT - GOLD', 'CCMAHA-MSE CGT-NC<=25L', 'IDFC FIRST Power - 10K', 'SAVINGS ACCOUNT-UPSTOX', 'CA-GEN-SOC/BANKS-URBAN', 'SBCHQ-GEN-PUB-SEMI URB', 'SAVINGS CHOICE ACCOUNT', 'SB-PRIORITY CUM SALARY', 'SA - SAVINGS ADVANTAGE', 'SAVING ACCOUNT-SAVINGS', 'OCC-TRADEWELL-MSE-REPO', 'SB-CHQ-GEN-PUB-IND-ALL', 'OD PERSONAL LOAN STAFF', 'SAVING BANK INDIVIDUAL', 'SBCHQ-STF-NONRURAL-INR', 'PMMY MICRO ENTERPRISES', 'SBINDU-SAVINGS ACCOUNT', 'SAVINGS BANK - SALARY A', 'CHEQUE SAVINGS DEPOSITS', 'BARODA ADVANTAGE SB_GEN', 'REGULAR SB CHQ-ENTITIES', 'CA - BUSINESS ADVANTAGE', 'SAVINGS ACCOUNT-KOTAK 3', 'CURRENT ACCOUNT-CURRENT', 'HSS-GEN-STF-IND-ALL-INR', 'LIBERTY SAVINGS ACCOUNT', 'PRESTIGE SALARY ACCOUNT', 'CURRENT ACCOUNT-PREMIUM', 'CA - BUSINESS PRIVILEGE', 'CA-GOLD-PUB-OTH-ALL-INR', 'SAVINGS-DIGITAL ACCOUNT', 'INSTA PLUS- NCHQ SB A/C', 'CURRENT ACCOUNT GENERAL', 'SAVING ACCOUNT- GENERAL', 'PrathamAccount (BSBDA)', 'SBCHQ-GEN-PUB-RURAL-IND', 'CURRENT ACCOUNT REGULAR', 'Startup Current Account', 'GENERAL CURRENT ACCOUNT', 'Regular Saving Accounts', 'SPECIAL SAVING ACCOUNTS', 'CLASSIC SAVINGS ACCOUNT', 'SAVING BANK A/C - WOMEN', 'SPECIAL CURRENT ACCOUNT', 'CURRENT ACCOUNT-FREEDOM', 'REGULAR CURRENT ACCOUNT', 'HSS--SALARY-IND-ALL-INR', 'SBCHQ-PEN-PUB-METRO-INR', 'SAVING ACCOUNT-RESIDENT', 'SBCHQ-GEN-PUB-URBAN-IND', 'EB-MSME-DROPLINE OD ABL', 'SB-ChqGeneral-Staff-All', 'BASIC SAVING BK DEP A/C', 'CA-RURAL-FIRM/TRUST/SOC', 'Current Account Account', 'CURRENT DEPOSIT [11001]', 'AGRICULTURE CASH CREDIT', 'IDFC FIRST Power - 10K', 'Insta- Salary - Classic', 'statement of transaction', 'MICRO-SMALL-ENT-OCC-REPO', 'CA-GEN-PUB-OTH-RURAL-INR', 'SB-CHQ-GEN-PUB-IND-RURAL', 'CURRENT ACCOUNT- GENERAL', 'Classic Corporate Salary', 'SBNCHQ-FIN INCLUSION-INR', 'PRESTIGE SAVINGS ACCOUNT', 'CA-GEN-PUB-IND-RURAL-INR', 'OD - STAFF - CLERK (NEW)', 'AXIS EASY SALARY ACCOUNT', 'BARODA ADVANTAGE CURRENT', 'SAVINGS ACCOUNTS REGULAR', 'SBNCHQ-GEN-PUB-URBAN-IND', 'CD-GEN-PUB-IND-RURAL-INR', 'SBNCHQ-GEN-PUB-RURAL-IND', 'SB - FINANCIAL INCLUSION', 'Dynamic Business Account', 'Jana Banker Salary - 152', 'Standard Savings Account', 'OCC - OTHER PRIORITY SEC', 'REGULARSBCHQ-INDIVIDUALS', 'CURRENT ACCOUNT DEPOSITS', 'CD-GEN-PUB-OTH-METRO-INR', 'CD-GEN-PUB-IND-METRO-INR', 'SB-CHQ-GEN-PUB-IND-URBAN', 'CA - FOR STARTUP BANKING', 'CD-GEN-PUB-OTH-RURAL-INR', 'SAVINGS ACCOUNT - PUBLIC', 'Society Current Deposits', 'SB - SAVING BANK ACCOUNT', 'SAVINGS BANK ORD GEN PUB', 'LOTUS SAVING BANK-ADHAR-', 'SBNCHQ-PUB-IND-RURAL-INR', 'CD-GEN-PUB-IND-URBAN-INR', 'CD-GEN-PUB-OTH-URBAN-INR', 'SBNFRL-PUB-IND-RURAL-INR', 'SAVING ACCOUNT-NEW YOUNG', 'Being Me Savings Account', 'BASIC SB DEPOSIT ACCOUNT', 'CC-MAHA-MSE CGT<=25L_New', 'Regular Business account', 'Insta- Salary - Platinum', 'REGULAR SB CHQ-PENSIONERS', 'HSS-GEN-PUB-IND-RURAL-INR', 'BARODA BASIC SAVINGS BANK', 'CURRENT ACCOUNT - REGULAR', 'Corporate Salary Platinum', 'CURRENT ACCOUNT-INDUS MAX', 'HSS-GEN-PUB-IND-METRO-INR', 'Current Account - General', 'CA-SME POWER-POS -OTH-INR', 'SAVINGS-ACCOUNT FOR YOUTH', 'Cur-Gen-Pub-Corp-NonRural', 'SBCHQ-GEN-PUB-IND-PBB-INR', 'CURRENT DEPOSIT ELITE A/C', 'OD FACILITY ON CA ACCOUNT', 'GOVERNMENT SALARY ACCOUNT', 'CC-MAHAMSME NCGT<=25L_New', 'SBCHQ-RSP-PUBIND-GOLD-INR', 'BARODA ADVANTAGE SB_O_BAL', 'SAVING DEPOSIT INDIVIDUAL', 'SB-NCHQ-GEN-PUB-IND-RURAL', 'HSS-GEN-PUB-OTH-METRO-INR', 'HSS-GEN-PUB-IND-URBAN-INR', 'SBCH-CGSP-PUBIND-GOLD-INR', 'HSS-GEN-PUB-OTH-RURAL-INR', 'BARODA JEEVAN SURAKSHA SA', 'UNION MICRO DIGITAL C A/C', 'CC-UCO VYAPAR SAMRDH-MSME', 'SAVINGS BANK DEPOSIT(RES)', 'CA-SILVER-PUB-OTH-ALL-INR', 'SAVINGS BANK SALARY PRIVL', 'SBBASIC-PUB-IND-NON RURAL', 'SB-SalaryGain-Pub-Ind-All', 'BARODA ADVANTAGE SB AT BC', 'MC-TL-XPRESS CREDIT APR21', 'SB-ChqGeneral-Pub-Oth-All', 'SBCHQ-NRE-PUB IND-ALL-INR', 'SAVING ACCOUNT INDIVIDUAL', 'SBCHQ-DSP-PUB IND-GOLD-INR', 'REGULAR SB CHQ-INDIVIDUALS', 'CA-REGULAR-PUB-OTH-ALL-INR', 'EASY ACCESS SALARY ACCOUNT', 'SB-Chq General-Pub-IND-ALL', 'Insta- Salary - Classic DC', 'EASYACCESS SAVINGS ACCOUNT', 'SB TINY SPL OD GEN PUB IND', 'SBNCHQ-GEN-PUB-IND-PBB-INR', 'SBCHQ-PSP-PUB IND-GOLD-INR', 'HSS-NCHQ-PUB-IND-RURAL-INR', 'EMP Clean_OD_OFFICERS-RLLR', 'CA-GEN-PUB-METRO/URBAN-INR', 'CURRENT ACCOUNT-INDUS BLUE', 'HSS-GEN-PUB-IND-SEMI URBAN', 'SAVING ACCOUNT-INDUS BASIC', 'BASIC SAVINGS BANK ACCOUNT', 'Jana Savings Account - 123', 'Super Shakti Women Account', 'SB NO FRILLS BC Non Cheque', 'GSS CC-PMEGP-AGRI -MCLR 1Y', 'SAVINGS ACCOUNT- SEMIURBAN', 'BURGUNDY - SAVINGS ACCOUNT', 'REGULAR SB NCHQ-PENSIONERS', 'PMJDY KIOSK Currency : INR', 'CA-CURRENT ACCOUNT DEPOSIT', 'JIFFY SALARY ACCOUNT STAFF', 'CURRENT ACCOUNT - START UP', 'OD Cent Business Loan MSME', 'SBCHQ-SALDIS-PUB-METRO-INR', 'Corporate Salary Platinum', 'SBCHQ-SGSP-PUB IND-GOLD-INR', 'Sav-Chq-Yuva-Pub-Ind-AllINR', 'SBCHQ-GEN-PUB IND-RURAL-INR', 'SBCHQ-CAPSP-PUBIND-GOLD-INR', 'REGULAR SB NCHQ-INDIVIDUALS', 'SBCHQ-GEN-PUB-IND-RURAL-INR', 'SBCHQ-CSA-PUBIND-CONTSILVER', 'CA-GEN-PUB-OTH-NonRural-INR', 'SBCHQ-CSA-PUB-IND-CSDMD-INR', 'CA-GEN-PUB-IND-NONRURAL-INR', 'AXIS LIBERTY SALARY ACCOUNT', 'CA-GEN-PUB OTH-NONRURAL-INR', 'CURRENT ACCOUNT-BFIL TATKAL', 'Insta- Salary - Platinum DC', 'SL CHQ-GEN-PUB-SU/RURAL-INR', 'SBCHQ-RSP-PUBIND-SILVER-INR', 'SBNCHQ-PUB-IND-NONRURAL-INR', 'Merchant Multiplier Account', 'Maha Sarvajan SB W/o Cheque', 'SBNCHQ-GEN-PUB-IND-NONRURAL', 'SB TINY-GEN-PUB IND-ALL-INR', 'EMP Clean OD-SUB STAFF-RLLR', 'Metro Savings Account - 170', 'SAVING ACCOUNT-INDUS DELITE', 'Equitas Eva-Savings Account', 'SA - SMART SALARY EXCLUSIVE', 'SAVING ACCOUNT-INDUS MAXIMA', 'AXIS LIBERTY SAVING ACCOUNT', 'SBCH-CGSP-PUBIND-SILVER-INR', 'CA-ARTHIA (COMM. AGENT)-INR', 'YES PRAGATI CURRENT ACCOUNT', 'SBCHQ-CSA-PUB-IND-CSPLT-INR', 'SB-INSTA-NCHQ-RURAL-PUB-IND', 'SB-WChq-Pens-Pub-Ind-AllINR', 'REGULAR SAVINGS BANK ACCOUNT', 'DIGITAL SAVING BANK -IND-INR', 'SBCHQ-CSA-PUB IND-CSGOLD-INR', 'SBCHQ-GEN-PUB-SEMI URB/RUR-I', 'LOTUS SAVING BANK-ADHAR- CHQ', 'SB-W/oChq-Gen-Pub-Ind-AllINR', 'SBCHQ-GEN-PUB-SEMI-URBAN-IND', 'SBNCHQ-GEN-PUB-IND-RURAL-INR', 'SBCHQ-DSP-PUB IND-SILVER-INR', 'Maha Sarvajan SB with Cheque', 'SAVING ACCOUNT-INDUS COMFORT', 'AXIS REPUBLIC SALARY ACCOUNT', 'Cur-Gen-Pub-Ind-NonRural-INR', 'Zero Balance Savings Account', 'SB-w/o-Chq-Bk-YUVA-INSTACARD', 'CURRENT A/C - COLLECTION A/C', 'SAVING ACCOUNT-INDUS CLASSIC', 'EB-MSME-OD-USUAL CREDIT DISP', 'Cur-Gen-Pub-Ind-Rural-SU-INR', 'SBCHQ-RSP-PUBIND-DIAMOND-INR', 'SBCHQ-ICGSP-PUB IND-GOLD-INR', 'DIGITAL SAVINGS BANK REGULAR', 'SBCH-CGSP-PUBIND-DIAMOND-INR', 'SAVING ACCOUNT-UPSTOX 3 IN 1', 'CURRENT ACCOUNT-INDUS SILVER', 'CC Cent Mudra under Priority', 'SBCHQ-SBP-PEHLIUDAAN (M)-INR', 'SBCHQ-GEN-PUB-METRO/URBAN-IN', 'Pimpri Chinchwad/SB/GEN/9631', 'CD-GEN-PUB-IND-SEMIURBAN-INR', 'PRIME Salary Savings account', 'Greater Sadhan SB Scheme-Pub', 'CA-GEN-PUB-IND-NO NRURAL-INR', 'SBNCHQ-GEN-PUB IND-RURAL-INR', 'EB-MSME-CC-STANDUP INDIA SUI', 'CA-GEN-PUB OTH-CSA REIMB-INR', 'SAVINGS BANK - GENERAL URBAN', 'World Business Account - 50K', 'SBCHQ-CSA-PUBIND-CSSILVER-INR', 'SMART BANKING SAVINGS ACCOUNT', 'SAVINGS-BASIC SAVINGS ACCOUNT', 'LOTUS SAVING BANK-ADHAR- NCHQ', 'LOTUS SAVING BANK AL OVD- CHQ', 'SBCHQ-CSA-PUB IND-CONT SILVER', 'SBCHQ-CAPSP-PUBIND-SILVER-INR', 'Cur-Gen-Pub-Corp-oth-Rural-SU', 'CURRENT ACCOUNT FOR JEWELLERS', 'SBCHQ-PSP-PUB IND -SILVER-INR', 'SBCHQ-GEN-PUB-METRO/URBAN-INR', 'SBCHQ-SGSP-PUBIND-DIAMOND-INR', 'SAVINGS ACCOUNT - GROUP STAFF', 'INDUS FREEDOM PREPAID ACCOUNT', 'LOTUS SAVING BAK AL OVD- NCHQ', 'SBCHQ-DSP-PUB IND-DIAMOND-INR', 'SB-Mahabank Salary Saving Acc', 'SBCHQ-SBP-GEN-PUB-IND-ALL-INR', 'SAVINGS DEPOSITS (INDIVIDUAL)', 'CURRENT DEPOSITS (INDIVIDUAL)', 'SAVINGS BANK - SENIOR CITIZEN', 'HSS-GEN-PUB-OTH-SEMIURBAN-INR', 'CA DEPOSIT NORMAL ACCOUNT-ENT', 'CC-MAHAMSME GST CREDIT SCHEME', 'SB Regular 25k (Asset X-Sell)', 'SBCH-CGSP-PUBIND-PLATINUM-INR', 'SBNCHQ-GEN-PUB-SEMI-URBAN-IND', 'Jiffy Zero Balance With Sweep', 'CD-CENT-SAKSHAM-OTH-RURAL-INR', 'JIFFY ZERO BALANCE WITH SWEEP', 'SBCHQ-SGSP-PUB IND -SILVER-INR', 'CA-GEN-FIRMS/COMPANY-SEMI-URBN', 'SB TINY SPL-OD-GEN-PUB IND-ALL', 'HSS-GEN-PUB-IND-SEMI URBAN-INR', 'SBNCHQ-GEN-PUB-METRO/URBAN-INR', 'EB-MSME-CC-USUAL CREDIT DISPEN', 'SBCHQ-GEN-PUB-SEMI URB/RUR-INR', 'SB-CORP-PAYROLL SALPACK SCHEME', 'CA-GEN-PUB-SEMIURBAN/RURAL-INR', 'SBCHQ-GEN-PUB-IND-NONRURAL-INR', 'SAVING ACCOUNT-INDUS PRIVILEGE', 'SB NONCHQ-BASIC SB DEPOSIT-INR', 'IB SB-FI-JHANDHAN-IND-RURAL-IN', 'SBChqMahabank Govt Zero BalSch', 'SBCHQ-CAPSP-PUBIND-DIAMOND-INR', 'HSS-GEN-PUB-IND-ONLINE-INB-INR', 'SB NONCHQ-GEN-PUB-SU/RURAL-INR', 'SBCHQ-PSP-HOMEGUARD-SILVER-INR', 'SBCHQ-CSA-PUB IND-CSSILVER-INR', 'SAVINGS-WOMENS SAVINGS ACCOUNT', 'SBCHQ-DSP-PUB IND-SAILOR-<18YR', 'CURRENT DEPOSITS - GEN [10041]', 'SBCHQ-SGSP-PUBIND-PLATINUM-INR', 'EB-DOD-PRE APRVD BSNS LON(PABL', 'CAPBG- PRIORITY CURRENT ACCOUNT', 'SAVINGS DEPOSIT - (GEN) [10001]', 'SAVING ACCOUNT-INDUS MULTIPLIER', 'SAVING ACCOUNT-INDUS DIGI-START', 'Principal Balance - SB - Regular', 'SB-ACCOUNT FOR COMMISSION AGENTS', 'PRESTIGE BANKING SAVINGS PROGRAM', 'CURRENT ACCOUNT FOR DISTRIBUTORS', 'CHQ. SAVING DEPOSIT (INDIVIDUAL)', 'SAVING DEPOSIT INDIVIDUALS [0017]', 'CANARA SB PREMIUM PAYROLL- SILVER', 'SB-CHQ-GEN-PUB-IND Currency : INR', 'CANARA SB PREMIUM PAYROLL - SILVER', 'Basic Savings Bank Deposit Account', 'SBNCHQ-GEN-PUB-RURAL-IND Currency :', 'CANARA SUPER SAVINGS SALARY ACCOUNT', 'SAVING ACCOUNT VEHICLE LOAN CUSTOMER', 'Principal Balance - CA Plus - Retail', 'CA- LARGE RETAILERS AND DISTRIBUTORS', 'YES PROSPERITY PRIME SAVINGS ACCOUNT', 'Savings Account Indus Comfort Maxima', 'SB-CHQ-GEN-PUB-IND-RURAL Currency : INR', 'CANARA BASIC SAVINGS BANK DEPOSIT ACCOUN', 'DIGITAL SAVINGS ACCOUNT - WITHOUT LIMITS', 'Principal Balance - CA Regular-Corporate', 'PRIORITY BANKING DIGITAL SAVINGS ACCOUNT', 'CSB Orange - Current Account - Corporate', 'HSBC Account Statement SAVINGS ACCOUNT - RES', 'CA DIGITAL PROPRIETORSHIP TRANSACTION ACCOUNT']

def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

async def authenticate_user(username: str, password: str):
    user = await get_user(username)
    if not user:
        return False
    if not verify_password(password, user.hashed_password):
        return False
    return user

async def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, QUALITY_SECRET, algorithm=ALGORITHM)
    return encoded_jwt

INCON_DATA_LOCK_FILE = '/tmp/incon_cron_job.lock'
async def send_inconsistent_data(date_ranges=[]):
    if os.path.exists(INCON_DATA_LOCK_FILE):
        print("Another instance is already running, skipping for this worker")
        return
    
    with open(INCON_DATA_LOCK_FILE, "w") as f:
        pass

    time.sleep(random.randint(1, 10))
    job_executed = redis_cli.get("job_executed")
    if job_executed:
        print("Send Inconsistent Data already Scheduled")
        os.remove(INCON_DATA_LOCK_FILE)
        return
    redis_cli.set("job_executed", 1, 600)
    print("Send Inconsistent Data Scheduled")
    try:
        fetch_for = [(datetime.now()-timedelta(1)).strftime("%Y-%m-%d")]
        if date_ranges:
            fetch_for = date_ranges
        
        query = f"""
            SELECT
                STATEMENT_ID,
                entity_id,
                max(api_key) api_key,
                account_id,
                attempt_type,
                bank_name,
                pdf_password,
                from_date,
                to_date,
                created_at,
                transaction_count,
                page_count
                FROM
                (
                    (
                    SELECT
                        *
                    FROM
                        (
                        SELECT
                            STATEMENT_ID,
                            bank_connect_entity.entity_id,
                            account_id,
                            attempt_type,
                            bank_name,
                            pdf_password,
                            from_date,
                            to_date,
                            to_char(s.created_at, 'YYYY-MM-DD') created_at,
                            transaction_count,
                            page_count,
                            pdf_hash,
                            organization_id,
                            row_number() OVER (
                            PARTITION BY lower(pdf_hash)
                            ORDER BY
                                s.created_at DESC
                            ) AS rnk
                        FROM
                            bank_connect_statement s
                            JOIN bank_connect_entity ON bank_connect_entity.id = s.entity_id
                        WHERE
                            is_complete = TRUE
                            AND is_extracted = TRUE
                            AND is_extracted_by_perfios = FALSE
                            AND to_char(s.created_at, 'YYYY-MM-DD') IN ({','.join([':item' + str(i) for i in range(len(fetch_for))])})
                            AND organization_id <> 1
                            AND attempt_type <> 'aa'
                        ) TEMP
                    WHERE
                        rnk = 1
                    )
                    UNION
                    (
                        SELECT
                        *
                        FROM
                        (
                            SELECT
                            STATEMENT_ID,
                            bank_connect_entity.entity_id,
                            account_id,
                            attempt_type,
                            bank_name,
                            pdf_password,
                            from_date,
                            to_date,
                            to_char(s.created_at, 'YYYY-MM-DD') created_at,
                            transaction_count,
                            page_count,
                            pdf_hash,
                            organization_id,
                            row_number() OVER (
                                PARTITION BY lower(pdf_hash)
                                ORDER BY
                                s.created_at DESC
                            ) AS rnk
                            FROM
                            bank_connect_statement s
                            JOIN bank_connect_entity ON bank_connect_entity.id = s.entity_id
                            WHERE
                            is_complete = TRUE
                            AND is_extracted = TRUE
                            AND is_extracted_by_perfios = FALSE
                            AND to_char(s.created_at, 'YYYY-MM-DD') IN ({','.join([':item' + str(i) for i in range(len(fetch_for))])})
                            AND organization_id <> 1
                            AND attempt_type = 'aa'
                        ) TEMP
                    )
                ) temp2
                JOIN users_user u ON temp2.organization_id = u.organization_id
                GROUP BY
                    STATEMENT_ID,
                    entity_id,
                    account_id,
                    attempt_type,
                    bank_name,
                    pdf_password,
                    from_date,
                    to_date,
                    created_at,
                    transaction_count,
                    page_count,
                    pdf_hash,
                    temp2.organization_id,
                    rnk
        """
        
        await portal_db.disconnect()
        print("portal db disconnected")
        await portal_db.connect()
        print("portal db re connected")
        
        total_uploads = await portal_db.fetch_all(query, values={f'item{i}': fetch_for[i] for i in range(len(fetch_for))})
        print("portal db query ran successfully")
        total_inconsistent = get_inconsistent_statements(fetch_for[0])
        print("clichouse db query ran successfully")
        if total_inconsistent is None:
            os.remove(INCON_DATA_LOCK_FILE)
            return
        
        final_query_data = []
        for single_upload in total_uploads:
            single_upload = dict(single_upload)
            if single_upload['statement_id'] in total_inconsistent:
                single_upload['is_inconsistent'] = True
            else:
                single_upload['is_inconsistent'] = False
            single_upload['from_date'] = single_upload['from_date'].strftime("%Y-%m-%d") if single_upload['from_date'] else None
            single_upload['to_date'] = single_upload['to_date'].strftime("%Y-%m-%d") if single_upload['to_date'] else None
            final_query_data.append(single_upload)

        grouped_by_attempt_type = group_by_key(final_query_data, 'attempt_type')
        result = {}
        attempt_inconsistent = {}
        for key, value in grouped_by_attempt_type.items():
            result[key] = {}
            result[key]['uploads'] = value
            grouped_by_inconsistent = group_by_key(value, 'is_inconsistent')
            result[key]['inconsistent'] = len(grouped_by_inconsistent.get(True, []))
            result[key]['total'] = len(value)
            result[key]['%'] = round(result[key]['inconsistent']/result[key]['total']*100, 2)
            if result[key]['inconsistent']:
                attempt_inconsistent[key] = result[key]['%']
        
        bank_wise_uploads = group_by_key(result.get('pdf', {}).get('uploads', []), 'bank_name')
        bank_wise_result = {}
        bank_inconsistent = {}
        for key, value in bank_wise_uploads.items():
            bank_wise_result[key] = {}
            grouped_by_inconsistent = group_by_key(value, 'is_inconsistent')
            bank_wise_result[key]['inconsistent'] = len(grouped_by_inconsistent.get(True, []))
            bank_wise_result[key]['total'] = len(value)
            bank_wise_result[key]['%'] = round(bank_wise_result[key]['inconsistent']/bank_wise_result[key]['total']*100, 2)
            if bank_wise_result[key]['inconsistent']:
                bank_inconsistent[key] = bank_wise_result[key]['%']

        attempt_wise_result = {
            "aa": {
                "inconsistent": result.get('aa', {}).get('inconsistent', 0),
                "total": result.get('aa', {}).get('total', 0),
                "%": result.get('aa', {}).get('%', 0)
            },
            "online": {
                "inconsistent": result.get('online', {}).get('inconsistent', 0),
                "total": result.get('online', {}).get('total', 0),
                "%": result.get('online', {}).get('%', 0)
            },
            "pdf": {
                "inconsistent": result.get('pdf', {}).get('inconsistent', 0),
                "total": result.get('pdf', {}).get('total', 0),
                "%": result.get('pdf', {}).get('%', 0)
            }
        }
        attempt_wise = pd.DataFrame.from_dict(attempt_wise_result, orient='index')
        attempt_wise.index.name = "attempt type"
        attempt_wise.reset_index(inplace=True)

        bank_wise = pd.DataFrame.from_dict(bank_wise_result, orient='index')
        if len(bank_wise)>0:
            bank_wise = bank_wise[bank_wise['inconsistent']!=0]
        bank_wise.index.name = "bank name"
        bank_wise.reset_index(inplace=True)
        
        file_name = f'{file_path}/{(datetime.now()-timedelta(1)).strftime("%Y-%m-%d")}_inconsistent.xlsx'
        writer = pd.ExcelWriter(file_name, engine='xlsxwriter')

        (pd.DataFrame(result.get('aa', {}).get('uploads', [])).replace([np.nan, 'nan'], '')).to_excel(writer, sheet_name="aa_uploads", index=False)
        (pd.DataFrame(result.get('online', {}).get('uploads', [])).replace([np.nan, 'nan'], '')).to_excel(writer, sheet_name="online_uploads", index=False)
        (pd.DataFrame(result.get('pdf', {}).get('uploads', [])).replace([np.nan, 'nan'], '')).to_excel(writer, sheet_name="pdf_uploads", index=False)
        attempt_wise.to_excel(writer, sheet_name="attempt_wise", index=False)
        bank_wise.to_excel(writer, sheet_name="bank_wise", index=False)

        writer._save()

        attempt_block = copy.deepcopy(attempt_wise)
        inconsistent_blocks = copy.deepcopy(bank_wise)
        inconsistent_blocks.sort_values(by='%', inplace=True, ascending=False)
        high_inconsistent_blocks = inconsistent_blocks[inconsistent_blocks['%']>=20]
        high_upload_blocks = inconsistent_blocks[inconsistent_blocks['%']<20]
        high_upload_blocks.sort_values(by='total', inplace=True, ascending=False)

        slack_message = f"INCONSISTENT ATTEMPTS\n{'-'*30}\n"
        slack_message += attempt_block.to_string(index=False)
        slack_message += f"\n\nHIGH INCONSISTENT BANKS\n{'-'*30}\n"
        slack_message += high_inconsistent_blocks.to_string(index=False)
        slack_message += f"\n\nHIGH VOLUME BANKS\n{'-'*30}\n"
        slack_message += high_upload_blocks.to_string(index=False)

        slack_client = WebClient(token=SLACK_TOKEN)
        date_format = '%b-%d, %y'
        show_date = ''
        if len(fetch_for)==1:
            show_date = datetime.strptime(fetch_for[0], '%Y-%m-%d').strftime(date_format)
        else:
            show_date = f"{datetime.strptime(fetch_for[0], '%Y-%m-%d').strftime(date_format)} - {datetime.strptime(fetch_for[-1], '%Y-%m-%d').strftime(date_format)}"
        response = slack_client.files_upload_v2(
            channel=SLACK_CHANNEL,
            initial_comment=f"{show_date} Inconsistent Data ```{slack_message}```",
            file=file_name
        )
        print("Inconsistent data sent successfully")
        if os.path.exists(file_name):
            os.remove(file_name)
    except Exception as e:
        sentry_sdk.capture_exception(e)
        print(f"Error sending inconsistent data: {e}")
    
    os.remove(INCON_DATA_LOCK_FILE)

def get_inconsistent_statements(fetch_for):
    query = """
        select
        distinct statement_id
        from
        (
            select
            *,
            dense_rank() over (
                partition by account_id
                order by
                created_at desc
            ) as rnk
            from
            bank_connect.disparity
            where
            formatDateTime(created_at, '%Y-%m-%d') >= {fetch_for:String} and toString(org_id) <> '1'
        ) temp
        where
        rnk = 1
        and inconsistent_type = 'Statement'
    """
    try:
        clickhouse_client = prepare_clickhouse_client()
        queried = clickhouse_client.query(query, parameters={'fetch_for': fetch_for})
    except Exception as e:
        sentry_sdk.capture_exception(e)
        return None
    statements = queried.result_rows
    for index in range(len(statements)):
        statements[index] = str(statements[index][0])
    return statements

def group_by_key(data, key):
    grouped_data = {}
    for item in data:
        group_key = item[key]
        if group_key not in grouped_data:
            grouped_data[group_key] = []
        grouped_data[group_key].append(item)
    return grouped_data

async def trigger_update_state(lambda_payload, username, entity_id, statement_id):
    random_number = str(uuid4())
    sqs_response = sqs_client.send_message(
            QueueUrl = RAMS_POST_PROCESSING_QUEUE_URL,
            MessageBody = json.dumps(lambda_payload),
            MessageDeduplicationId = '{}_{}_{}'.format(entity_id, statement_id, random_number),
            MessageGroupId = 'update_state_invocation_{}_{}'.format(statement_id, random_number)
        )

    query = """
        INSERT INTO retrigger_logs (
            retriggered_by,
            entity_id,
            statement_id,
            retrigger_type
            )
        VALUES (
            :retriggered_by,
            :entity_id,
            :statement_id,
            :retrigger_type
        )
    """

    await quality_database.execute(query=query, values={
        "retriggered_by": username,
        "entity_id": entity_id,
        "statement_id": statement_id,
        "retrigger_type": "Update State Fan Out"
    })

    return sqs_response