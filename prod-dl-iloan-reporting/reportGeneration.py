from decimal import Decimal
import json
import pytz
from pickle import FALSE, TRUE
from socket import timeout
from xmlrpc.client import Boolean
import requests
import pysftp
import os
from datetime import datetime
import threading
import psycopg2
from simple_salesforce import Salesforce, SalesforceLogin, SFType
from flask import Flask,jsonify,request
from dotenv import load_dotenv
import time
import traceback2 as traceback
import pandas as pd 
import subprocess

load_dotenv()

# ignore known host check
cnOpts = pysftp.CnOpts()
cnOpts.hostkeys = None
tz = pytz.timezone('Europe/Amsterdam')

def count_file_number():
    directory_path = '/tmp_to_sftp'
    No_of_files = len(os.listdir(os.getcwd()+directory_path))
    return No_of_files-1

def Save_File_To_Database(doc,file_data,conn) :
    curr = conn.cursor()
    Insert_script = 'INSERT INTO "Reports" ("Name", "File_Data") VALUES (%s, %s)'
    insert_value = (doc,file_data)
    curr.execute(Insert_script, insert_value)
    conn.commit()
    curr.close()

def purge_folder(folder):
    docs = os.listdir(os.getcwd() + '/' + folder + '/')
    for doc in docs:
        os.remove(os.getcwd() + '/' + folder + '/' + doc)
        print("Document deleted from local folder "+doc)


def upload_to_sftp1():
    try:
        # conn = psycopg2.connect(host=os.getenv("pghostname"), dbname=os.getenv("pgdatabase"), user=os.getenv("pgusername"), password=os.getenv("pgpassword"), port=os.getenv("pgportId"))
        host_name = os.getenv("host_name")
        server_user_name = os.getenv("server_user_name")
        privateKeyFilePath = './private_key.pem'
        docs = os.listdir(os.getcwd() + '/tmp_to_sftp/')
        print("Document Details===")
        print(docs)
        for doc in docs:
            if doc.endswith('.csv') :
                with pysftp.Connection(host=host_name, username=server_user_name, cnopts=cnOpts, private_key=privateKeyFilePath) as sftp:
                    print("Connection successfully established ....")
                    sftp.put(os.getcwd() + '/tmp_to_sftp/' + doc, '/Inbox/'+doc)
                    print("Uploaded to sftp", doc)
                    os.remove(os.getcwd() + '/tmp_to_sftp' + '/' + doc)
                    print("Document deleted from local folder "+doc)
                # with open("./tmp_to_sftp/"+doc,"r") as f:
                #     Save_File_To_Database(doc,f.read(),conn)
        # conn.close()
        #purge_folder('tmp_to_sftp')
    except KeyError as e:
        print("exception===="+e)
        raise e

def save_to_local(name,content):
    print("Inside save_to_local method")
    with open(os.getcwd() + '/tmp_to_sftp/' + name, "w") as output_file:
        output_file.write(content)
    print("Document saved successfully in local "+name)


def get_Report_Details(sf,reportId,reportName,sessionId):
    # Set report details
    sf_org = 'https://iloan-lloydsbank.my.salesforce.com/'
    export_params = '?csv=1&export=1&enc=UTF-8&isdtp=p1&xf=localecsv'

    # session = requests.Session()

    # Download report
    sf_report_url = sf_org + reportId + export_params
    print('Report Start Fetching...')
    try:
        if reportName == 'iLoan_Application Scorecard Criteria_D' or reportName == 'iLoan_Application Scorecard Criteria_d_D':
            response = requests.get(sf_report_url, headers=sf.headers, cookies={'sid': sessionId},timeout=120)
        else:
            response = requests.get(sf_report_url,headers=sf.headers, cookies={'sid': sessionId})
    except Exception as e :
        traceback.print_exc()
        print("exception Main====")
        print(e)
        print('Hit..')
        time.sleep(120)
        get_Report_Details(sf,reportId,reportName,sessionId)
        return None
    
    # Download report
    # sf_report_url = sf_org + reportId + export_params
    # response = requests.get(sf_report_url, headers=sf.headers, cookies={'sid': sessionId})
    print("Report Content Details Fetched from Salesforce")
    report_contents = response.content.decode('utf-8')
    report_contents = report_contents.replace("\n","\r\n")
    report_csv_name = file_Name_Pattern+datetime.now(tz).strftime("%Y%m%d")
    save_to_local(report_csv_name+'.csv',report_contents)
    if('UPL_PNA_Remediation_Expired_Data' in report_csv_name or 'UPL_PNA_Legal_Hold_list_D' in report_csv_name):
       createReportForRetention(report_csv_name)
       
#this method will create report for retention
def createReportForRetention(fileName):
    try:
        report_df = pd.read_csv(os.getcwd() + '/tmp_to_sftp' + '/' + fileName +'.csv',on_bad_lines='skip',sep=';')
        if('UPL_PNA_Legal_Hold_list_D' in fileName):
            report_df.columns = ['Loan number','Customer name','Reason','LHD date created','Batch date']
        else:
            legal_hold_record_list = []
            is_legal_hold = report_df['Legal Hold Flag']
            for i in range(len(is_legal_hold)):
                if is_legal_hold[i] == 0:
                    legal_hold_record_list.append('N')
                else:
                    legal_hold_record_list.append('Y')
            report_df = report_df.drop('Legal Hold Flag',axis=1)
            report_df['IND REMEDIATION'] = legal_hold_record_list
            report_df.columns = ['LOAN NBR','PA AREA','SOURCE','DATE LAST ACTIVE','DATE REMEDIATION','IND REMEDIATION']
        report_df.to_csv(os.getcwd() + '/tmp_to_sftp' + '/' + fileName + '.csv', index=False)
        print("Updated Document saved successfully in local "+fileName+ '.csv')
        upload_to_sftp1()
    except Exception as e:
        print(e)


def create_Control_File(report_file_name, column_name, is_check_sum_flag, control_file_header_name, inner_File_Name, field_label_name, file_name_pattern, file_name_prefix):
    lines = []
    check_sum = 0
    row_count = 0
    print("Started to create control file for "+report_file_name)
    with open("./tmp_to_sftp/"+report_file_name,"r") as f:
        lines = f.readlines()
    header_line = lines[0].replace("\n","").replace("\"","").split(";")
    
    
    if is_check_sum_flag : 
        index = header_line.index(column_name)
        for each_line in lines:
            row_count = row_count+1
            if row_count == 1:
                continue
            data = each_line.split(";")
            data[index] = data[index].replace('"',"").replace("\r","").replace(",",".")
            if len(str(data[index])) > 0:
                if((data[index].replace(".","").replace("-","").strip()).isnumeric()) :
                    check_sum = check_sum + Decimal(data[index])
    row_count = len(lines)-1

    dynamic_csv = '"' + inner_File_Name + datetime.now(tz).strftime("%Y%m%d") + ".csv" + '";"' + str(row_count) + '";"' + str(check_sum) + '";"' + field_label_name +'"'
    merged_data = control_file_header_name + "\r\n" + dynamic_csv
    control_file_name = file_name_prefix + file_name_pattern + datetime.now(tz).strftime("%Y%m%d")+'.csv'
    save_to_local(control_file_name, merged_data)

app = Flask(__name__)

def setupSalesforce(report_configuration_Id_List,is_Report_File) :
    try:
        username = os.getenv("sf_user_name")
        password = os.getenv("sf_password")
        security_token = os.getenv("sf_token")
        domain = os.getenv("sf_domain")

        print("Salesforce not Connected")
        sf = Salesforce(username=username, password=password, security_token=security_token)
        sessionid = sf.session_id
        print("Salesforce Connected")

        for report_configuration_Id in report_configuration_Id_List :
            report_configuration_details = sf.query(f"select Id, Name, Accounting_Ledger_Report__c, File_Prefix_Name__c, Column_Name__c, Report_Name__c, File_Name_Pattern__c, File_Header__c, Inner_File_Name__c, Field_Label_Name__c, Checksum_File__c, Monthly_Report_Creation__c from Reports_Configuration__c where Id = '{report_configuration_Id}'")
            record = report_configuration_details["records"][0]
            folder_name  = ""
            if Boolean(record['Monthly_Report_Creation__c']) :
                folder_name = "Financial Accounting Reports"
            elif Boolean(record['Accounting_Ledger_Report__c']) :
                folder_name = "Accounting"
            else :
                folder_name = "MIDB"

            report_Name = record['Report_Name__c']

            salesforce_report = sf.query(f"Select Id,name from Report Where FolderName = '{folder_name}' AND Name LIKE '%{report_Name}%'")
            report_details = salesforce_report["records"][0]
            global reportID
            global coulumn_Name
            global is_check_sum_flag
            global control_file_header_name
            global inner_File_Name
            global field_label_name
            global file_Name_Pattern
            global file_name_prefix
            reportID = report_details['Id']
            coulumn_Name = record['Column_Name__c']
            is_check_sum_flag = record['Checksum_File__c']
            control_file_header_name = record['File_Header__c']
            inner_File_Name = record['Inner_File_Name__c']
            field_label_name = record['Field_Label_Name__c']
            file_Name_Pattern = record['File_Name_Pattern__c']
            file_name_prefix = record['File_Prefix_Name__c']

            if is_Report_File : 
                get_Report_Details(sf,reportID,report_Name,sessionid)
            else :
                create_Control_File(file_Name_Pattern+datetime.now(tz).strftime("%Y%m%d")+'.csv',coulumn_Name,is_check_sum_flag, control_file_header_name,inner_File_Name, field_label_name, file_Name_Pattern, file_name_prefix)
        print('Total File - '+str(count_file_number()))
    except Exception as e:
        print(e)
        return {"message": "Error"}, 500
    
def restart_heroku_app(app_name):
    try:
        # Use subprocess to run the heroku restart command
        subprocess.run(['heroku', 'restart', '-a', app_name], check=True)
        print(f"Heroku app '{app_name}' restarted successfully.")
    except subprocess.CalledProcessError as e:
        # Handle errors, if any
        print(f"Error restarting Heroku app '{app_name}': {e}")

@app.route("/sendReport/",methods=['POST'])
def sendReportFile() :
    def startSession(**kwargs):
        try:
            report_configuration_Id_List = kwargs.get('requested_data', {})
            # restart_heroku_app('prod-dl-iloan-reporting')
            # time.sleep(120)
            # print('SetUpSalesforce is called')
            setupSalesforce(report_configuration_Id_List,True)
        except Exception as e:
            print(e)
    recordList = request.json['id'].split(",")       
    thread = threading.Thread(target=startSession, kwargs={
                    'requested_data': recordList})
    thread.start()
    #setupSalesforce(report_config_Id,True)
    return {"message": "Accepted"}, 200

@app.route("/sendControlFile/",methods=['POST'])
def sendControlReportFile() :
    def startSession(**kwargs):
        try:
            report_configuration_Id_List = kwargs.get('requested_data', {})
            setupSalesforce(report_configuration_Id_List, False)
        except Exception as e:
            print(e)
    recordList = request.json['id'].split(",")              
    thread = threading.Thread(target=startSession, kwargs={
                    'requested_data': recordList})
    thread.start()
    return {"message": "Accepted"}, 200

@app.route("/uploadToSftp/",methods=['GET'])
def uploadFiles() :
    def startSession():
        try:
            upload_to_sftp1()
            
        except Exception as e:
            print(e)
            
    thread = threading.Thread(target=startSession, kwargs={})
    thread.start()
    return {"message": "Accepted"}, 200

@app.route("/checkSFTPConnection/",methods=['GET'])
def checkActiveServer() :
    host_name = os.getenv("host_name")
    server_user_name = os.getenv("server_user_name")
    privateKeyFilePath = './private_key.pem'
    with pysftp.Connection(host=host_name, username=server_user_name, cnopts=cnOpts, private_key=privateKeyFilePath) as sftp:
        print("Connection successfully established ....")

@app.route("/purgeFolder/",methods=['GET'])
def deleteLocalFiles():
    purge_folder('tmp_to_sftp')
if __name__ == "__main__":
    app.run(debug=True)







