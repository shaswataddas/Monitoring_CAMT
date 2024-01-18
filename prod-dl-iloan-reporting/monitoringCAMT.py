import os
import xml.etree.ElementTree as ET
import re
from simple_salesforce import Salesforce, SalesforceLogin, SFType
from datetime import date,datetime
from decimal import Decimal,ROUND_HALF_UP

lptSuccessList = []
lptFailList = []
ddiSuccessList = []
ddiFailList = []
otherTransanctionSuccessList = []
otherTransanctionFailList = []
manualPaymentLaiQueryList = []
manualPaymentAppQueryList = []
today = datetime.strptime(str(date.today()),"%Y-%m-%d")
print(today)

def printResut(recordList):
    if len(recordList):
        concatenated_str = "The list contains: " + ', '.join(map(str, recordList))
    else:
        concatenated_str = "No Data"
    return concatenated_str

def checkDataWithSalesforce(recordId, amount, type):
    try:
        # print('Salesforce not connected yet')
        username = 'system@iloan.nl.prd'
        password = '17!zSZSiKd4Hax3aSTfNIqRY'
        security_token = '8QJPLRnE0Xd83wTNHJsMLQkJ'
        sf = Salesforce(username=username, password=password, security_token=security_token)
        # print('salesforce connected')
        # sessionid = sf.session_id
        # sf_host = urlparse(sf.base_url).netloc
        # camt_file_id = camt_file_list[0]
        if(type == 'LPT'):
            lpt_detail = sf.query(f"select id,name, loan__Receipt_Date__c, loan__Clearing_Date__c, Value_Date__c, loan__Transaction_Creation_Date__c, loan__Cleared__c, loan__Transaction_Amount__c from loan__Loan_Payment_Transaction__c where name = '{recordId}'")
            clearing_date = datetime.strptime(lpt_detail["records"][0]['loan__Transaction_Creation_Date__c'],"%Y-%m-%d")
            check_clear = lpt_detail["records"][0]['loan__Cleared__c']
            transaction_amount = Decimal(lpt_detail["records"][0]['loan__Transaction_Amount__c'])
            transaction_amount = transaction_amount.quantize(Decimal('0.00'), rounding=ROUND_HALF_UP)
            if check_clear == True and clearing_date == today and transaction_amount == amount:
                lptSuccessList.append(recordId)
            else:
                lptFailList.append(recordId)
        elif(type == 'DDI'):
            ddi_details = sf.query(f"select id,name, Booking_Date__c, Value_Date__c, Clear__c,loan__Distribution_Amount__c from loan__Disbursal_Txn_Distribution__c where name = '{recordId}'")
            clearing_date = datetime.strptime(ddi_details["records"][0]['Value_Date__c'],"%Y-%m-%d")
            check_clear = ddi_details["records"][0]['Clear__c']
            transaction_amount = Decimal(ddi_details["records"][0]['loan__Distribution_Amount__c'])
            transaction_amount = transaction_amount.quantize(Decimal('0.00'), rounding=ROUND_HALF_UP)
            if check_clear == True and clearing_date == today and transaction_amount == amount:
                ddiSuccessList.append(recordId)
            else:
                ddiFailList.append(recordId)
        elif(type == 'OLTID'):
            otherTransaction_details = sf.query(f"select id,name, loan__Txn_Amt__c, loan__Transaction_Creation_Date__c from loan__Other_Transaction__c where name = '{recordId}'")
            transaction_amount = Decimal(otherTransaction_details["records"][0]['loan__Txn_Amt__c'])
            transaction_amount = transaction_amount.quantize(Decimal('0.00'), rounding=ROUND_HALF_UP)
            if transaction_amount == amount:
                otherTransanctionSuccessList.append(recordId)
            else:
                otherTransanctionFailList.append(recordId)

    except Exception as e:
        print('error occured due to')
        print(e)
        return{"message":e},500


def returnRootValue(tags):
    root = ET.fromstring(tags)
    ustrd_value = root.text
    return ustrd_value

def findNtry(file_content):
    # Finding the NTRY pattern
    pattern = re.compile(r'<ntry>.*?</ntry>', re.DOTALL | re.IGNORECASE)
    ustrd_pattern = re.compile(r'<Ustrd>.*?</Ustrd>', re.DOTALL | re.IGNORECASE)
    pmtInfId_pattern = re.compile(r'<PmtInfId>.*?</PmtInfId>', re.DOTALL | re.IGNORECASE)
    amount_pattern = re.compile(r'<Amt Ccy="EUR">.*?</Amt>', re.DOTALL | re.IGNORECASE)
    rvslInd_pattern = re.compile(r'<RvslInd>.*?</RvslInd>', re.DOTALL | re.IGNORECASE)
    ntry_tags = re.findall(pattern, file_content)
    for ntry_tag in ntry_tags:
        ustrd_tags = re.findall(ustrd_pattern, ntry_tag)
        PmtInfId_tags = re.findall(pmtInfId_pattern, ntry_tag)
        amount_tags = re.findall(amount_pattern, ntry_tag)
        rvslInd_tags = re.findall(rvslInd_pattern, ntry_tag)
        if(ustrd_tags and ustrd_tags[0].strip() != ""):
            ustrd_value = returnRootValue(ustrd_tags[0])
            lai_pattern = re.compile(r'\bLAI-\w+')
            app_no_pattern = re.compile(r'^1\d{7}$')
            # manualPaymentLaiQueryList.append(re.findall(lai_pattern, ustrd_value)[0])
            # manualPaymentAppQueryList.append(re.findall(app_no_pattern, ustrd_value)[0])
        elif(PmtInfId_tags and PmtInfId_tags[0].strip() != ""):
            pmntInfo_value = returnRootValue(PmtInfId_tags[0])
            amount_value = Decimal(returnRootValue(amount_tags[0]))
            if(str(pmntInfo_value)[0:3]=='LPT'):
                checkDataWithSalesforce(pmntInfo_value,amount_value,'LPT')
            elif(str(pmntInfo_value)[0:3]=='DDI'):
                checkDataWithSalesforce(pmntInfo_value,amount_value,'DDI')
            elif(str(pmntInfo_value)[0:5]=='OLTID'):
                checkDataWithSalesforce(pmntInfo_value,amount_value,'OLTID')
    print(manualPaymentLaiQueryList)
    print(manualPaymentAppQueryList)
    print('Success LPT - '+printResut(lptSuccessList))
    print('Failed LPT - '+printResut(lptFailList))
    print('Success DDI - '+printResut(ddiSuccessList))
    print('Failed DDI - '+printResut(ddiFailList))
    print('Success Other Transaction - '+printResut(otherTransanctionSuccessList))
    print('Failed Other Transaction - '+printResut(otherTransanctionFailList))


def readCAMT() :
    file_prefix = 'CAMT'
    current_dir = os.getcwd()
    folder_name = 'tmp_to_sftp'
    all_files = os.listdir(os.path.join(current_dir, folder_name))
    matching_files = [file for file in all_files if file.startswith(file_prefix)]
    if matching_files:
        file_to_read = matching_files[0]

        # Construct the full file path
        file_path = os.path.join(os.path.join(current_dir, folder_name), file_to_read)

        # Open and read the file
        with open(file_path, 'r') as file:
            content = file.read()
            print('Successfully Read the CAMT File')
        return content
    else:
        print(f"No file found with the prefix '{file_prefix}' in the current directory.")

if __name__ == "__main__":
    file_content = readCAMT();
    findNtry(file_content)
