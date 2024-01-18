from decimal import Decimal
from urllib.parse import urlparse
import json
import base64
import socks
import pytz
from pickle import FALSE, TRUE
from socket import timeout
from xmlrpc.client import Boolean
import requests
import pysftp
import os
import shutil
import zipfile
from datetime import datetime
import threading
import psycopg2
from simple_salesforce import Salesforce, SalesforceLogin, SFType
from flask import Flask,jsonify,request
from dotenv import load_dotenv
from asyncore import write
from base64 import encode
import xml.etree.ElementTree as ET
from jinja2 import Undefined
from datetime import date


app = Flask(__name__)
load_dotenv()
cnOpts = pysftp.CnOpts()
cnOpts.hostkeys = None
tz = pytz.timezone('Europe/Amsterdam')
def setupSalesforce(camt_file_list) :
    try:
        print('not connected yet')
        username = str(os.getenv("sf_user_name"))
        password = str(os.getenv("sf_password"))
        security_token = str(os.getenv("sf_token"))
        os.environ['http_proxy'] = os.getenv('QUOTAGUARDSTATIC_URL')
        proxies = {
        "http": os.getenv('QUOTAGUARDSTATIC_URL'),
        "https": os.getenv('QUOTAGUARDSTATIC_URL')
        }
        sf = Salesforce(username=username, password=password, security_token=security_token,proxies=proxies)
        print('salesforce connected')
        sessionid = sf.session_id
        sf_host = urlparse(sf.base_url).netloc
        camt_file_id = camt_file_list[0]
        camt_file_details = sf.query(f"select Id, Name,Body,Description from Document where Id = '{camt_file_id}'")
        record = camt_file_details["records"][0]
        IloanConfigurableSetting = sf.query(f"select Number_of_Ntry_to_be_processed__c,Header_For_CAMT_file__c,CAMT_footer__c,Stmt_opening_tag__c,Stmt_Closing_tag__c from Special_Data_enable__c")
        noOfEntries = int(IloanConfigurableSetting["records"][0]['Number_of_Ntry_to_be_processed__c'])
        header = IloanConfigurableSetting["records"][0]['Header_For_CAMT_file__c']
        footer = IloanConfigurableSetting["records"][0]['CAMT_footer__c']
        openingStmt = IloanConfigurableSetting["records"][0]['Stmt_opening_tag__c']
        closingStmt = IloanConfigurableSetting["records"][0]['Stmt_Closing_tag__c']
        docDetails = getDocumentContent(record,sf_host,sessionid)
        saveToLocal(docDetails["name"], docDetails["content"])
        unzipFiles()
        deleteTempFolder()
        breakSingleCamtFile(noOfEntries,header,footer,openingStmt,closingStmt)
        upload_to_sf(sf)
        emptyFolder()
        return "Uploaded"
    except Exception as e:
        print('error occured due to')
        print(e)
        return{"message":e},500

def getDocumentContent(document,sf_host,sessionId):
    url = "https://"+sf_host+document["Body"]
    docDownload = requests.get(url,headers={
        "Authorization":"OAuth "+sessionId
    })
    return {
        "content":docDownload.content,
        "name":"camt.zip"
    }


def saveToLocal(name,content):
    with open('local_folder/'+name,'wb') as output_file:
        output_file.write(content)


def unzipFiles():
    with zipfile.ZipFile('local_folder/camt.zip','r') as zip_ref :
        zip_ref.extractall('local_folder_unzipped_files')

def upload_to_sf(sf):
    rootdir = 'local_folder_divided_files'
    count = 0
    today = date.today()
    date_today = today.strftime("%d_%m_%Y")
    for file in os.listdir(rootdir):
        count = count+1
        d = os.path.join(rootdir,file)
        with open(d,mode='rb') as file:
            unzipped_camt_file = file.read()
        unzipped_camt = base64.encodebytes(unzipped_camt_file).decode('utf-8')
        unzipped_camt_folder = sf.query(f"select Id, Name from Folder where Name = 'Received Payment Files'")
        folderRecord = unzipped_camt_folder["records"][0]
        folderRecordId = folderRecord['Id']
        sf.Document.create({'folderId':folderRecordId,'name':'camtfile_'+str(count)+'_'+date_today+'.xml','body':unzipped_camt,'ContentType':'application/xml'})

def deleteTempFolder():
    rootdir1 = 'local_folder'
    for file in os.listdir(rootdir1):
        d = os.path.join(rootdir1,file)
        if os.path.isdir(d):
            shutil.rmtree(d)
    rootdir2 = 'local_folder_unzipped_files'
    for file in os.listdir(rootdir2):
        d = os.path.join(rootdir2,file)
        if os.path.isdir(d):
            shutil.rmtree(d)
        elif (file == 'header.xml'):
            os.remove(d)
    rootdir3 = 'local_folder_divided_files'
    for file in os.listdir(rootdir3):
        d = os.path.join(rootdir3,file)
        if os.path.isdir(d):
            shutil.rmtree(d) 

def emptyFolder():
    rootdir1 = 'local_folder'
    rootdir2 = 'local_folder_unzipped_files'
    rootdir3 = 'local_folder_divided_files'
    for file in os.listdir(rootdir1):
        d1 = os.path.join(rootdir1,file)
        os.remove(d1)
    for file in os.listdir(rootdir2):
        d2 = os.path.join(rootdir2,file)
        os.remove(d2)
    for file in os.listdir(rootdir3):
        d3= os.path.join(rootdir3,file)
        os.remove(d3)


def breakSingleCamtFile(noOfNtry,header,footer,openingStmt,closingStmt):
    rootdir2 = 'local_folder_unzipped_files'
    for file in os.listdir(rootdir2):
        camtFile = os.path.join(rootdir2,file)
    today = date.today()
    date_today = today.strftime("%d_%m_%Y")
    count = 0
    fileName = 'local_folder_divided_files/camtfile_'+str(count)+'_'+date_today+'.xml'
    firstElement = Undefined
    footerAdded = False
    ntryFound = False
    ET.register_namespace('xmlns',"urn:iso:std:iso:20022:tech:xsd:camt.053.001.02")
    ET.register_namespace('',"urn:iso:std:iso:20022:tech:xsd:camt.053.001.02")
    ET.register_namespace('xsi', "http://www.w3.org/2001/XMLSchema-instance")
    tree = ET.parse(camtFile)
    root = tree.getroot()
    tempXMLBody = ''
    for elem in root:
        for subelem in elem:
            if(subelem.tag.__contains__('GrpHdr')):
                firstElement = ET.tostring(subelem)
                elementBody = firstElement.decode()
    for elem in root:
        for subelem in elem:
            if(subelem.tag.__contains__('Stmt')):
                ntryFound = False
                count=count+1
                fileName = 'local_folder_divided_files/camtfile_'+str(count)+'_'+date_today+'.xml'
                XMLBody = header+elementBody+openingStmt
                ntryCount = 0
                for sub2elem in subelem:
                    if(sub2elem.tag.__contains__('Ntry')):
                        ntryFound = True
                        ntryCount = ntryCount+1
                        if(ntryCount == 1):
                            footerAdded = False
                            with open(fileName,'wb') as f:
                                f.write(tempXMLBody.encode())
                        with open(fileName,'ab') as f:
                            f.write(ET.tostring(sub2elem))
                        if(ntryCount == noOfNtry):
                            ntryCount = 0  
                            footerAdded = True
                            with open(fileName,'ab') as f:
                                f.write(closingStmt.encode())
                                f.write(footer.encode())
                            count = count+1
                            fileName = 'local_folder_divided_files/camtfile_'+str(count)+'_'+date_today+'.xml'
                    else:
                            XMLBody +=ET.tostring(sub2elem).decode()
                            tempXMLBody = XMLBody
                if(not ntryFound):
                    tempXMLBody = ''
                    XMLBody += closingStmt+footer
                    with open(fileName,'ab') as f:
                        f.write(XMLBody.encode())
                elif(ntryFound & (not footerAdded)):
                    with open(fileName,'ab') as f:
                        f.write((closingStmt+footer).encode())



@app.route("/sendcamtfiles/",methods=['POST'])
def sendcamtfiles() :
        recordList = request.json['id'].split(",")
        message = 'Something went wrong'
        try:
            camtFileIDList = recordList
            message = setupSalesforce(camtFileIDList)
        except Exception as e:
            print('error occured due to')
            print(e)
            emptyFolder()
        if(message == "Uploaded"):
            return{"message":message},200
        return{"message": message},500

if __name__ == "__main__":
    app.run(debug=True)
