import codecs
import pandas as pd
import requests
import urllib3
import time
import json
import csv
import sys
import math
import datetime
import smtplib
import argparse
from pathlib import Path
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
from email import encoders

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
requests.packages.urllib3.add_stderr_logger()

def request(method, path=None, data=None, params=None, uri=None, auth=None, content_type="application/json", accept="application/json", private_api=False, app_api=False ):
    headers = {}
    if token:
        headers['Authorization'] = "Bearer " + token
    if auth:
        headers['Authorization'] = auth
    if content_type:
        headers['Content-Type'] = content_type
    if path:
        uri = cluster_ip_or_fqdn+path
    if private_api:
        uri = path #If private API, do not append IRIS URL
    if data:
        payload = json.dumps(data)  
    else:
        payload = None
    if params:
        params=params

    if app_api:
        # If private API, use session to store cookie received
        r = app_session.request(method, uri, headers=headers,params=params,
                         data=payload, verify=False)
    else:
        r = requests.request(method, uri, headers=headers,params=params,
                         data=payload, verify=False)

    if 200 <= r.status_code <= 299:
        return r

    raise Exception(
        "Unsupported HTTP status code (%d) encountered" % r.status_code)

#Get Cohesity Cluster token and store it in environment variable
def get_iris_token(username, org, password):
    data = {
        "password": password,
        "username": username,
        "domain": org
    } 
    r = request('POST', 'public/accessTokens', data=data)
    global token
    token = json.loads(r.content)['accessToken']

#Get Cohesity Cluster token and store it in environment variable
def get_app_token():
    r = request('GET', 'public/appInstances')
    global app_token
    global app_nodePort
    all_apps =  json.loads(r.content)
    for app in all_apps:
        # Get Token for Insight App. Tokens are different for different apps
        if app['appName'] == "Insight" and app['healthStatus']=='kHealthy' and app['state']=='kRunning':
            app_token = app['appAccessToken']
            app_nodePort = app['nodePort']
            break

def get_filepaths(cluster_endpoint, search, pattern):
    
    # Set Coookie for Insight App using the below API Call 
    r = request('POST', 'https://' + cluster_endpoint + ':' + str(app_nodePort) + '/api/v1/login', data={'token': app_token}, private_api=True, app_api=True)
    
    previous_object_ids = set()
    search_result = []
    #Default Search string. Edit this as needed
    if(search is None):
        search_string = "/" + pattern + "/"
    else:
        search_string = search

    query_string = "(text:{0} OR text.emailindex:{0} OR attachment.content:{0}) AND (fileType:txt)".format(search_string)

    search_body = {
                "_source": {
                    "excludes": [
                    "attachment.content",
                    "text",
                    "text.emailindex"
                    ]
                },
                "query": {
                    "query_string": {
                    "query": query_string,
                    "fields": [
                        "text",
                        "text.emailindex",
                        "attachment.content"
                    ]
                    }
                },
                "from": 0,
                "size": 100
                }
    
    # Get search results
    r = request('POST', 'https://' + cluster_endpoint + ':' + str(app_nodePort) +'/api/v1/search/cohesity_insight_index_*/_search', data=search_body, private_api=True, app_api=True)
    hits = json.loads(r.content)['hits']['hits']
    
    # Create table with protection object name where the file exists
    for hit in hits:
        if hit['_source']['sourceId'] in previous_object_ids:
            
            for result in search_result:
                if (result['objectId'] == hit['_source']['sourceId']):
                    result['filePath'].append(hit['_source']['filePath'])
        elif hit['_source']['sourceId'] == 0:
            previous_object_ids.add(hit['_source']['sourceId'])
            temp_dict = {}
            temp_dict['vmName'], temp_dict['sourceName'], temp_dict['jobName'] = ["VIEW",hit['_source']['objectName'],"VIEW"]
            temp_dict['objectId'] = hit['_source']['sourceId']
            temp_dict['filePath'] = []
            temp_dict['filePath'].append(hit['_source']['filePath'])
            search_result.append(temp_dict)
        else:
            previous_object_ids.add(hit['_source']['sourceId'])
            temp_dict = {}
            temp_dict['vmName'], temp_dict['sourceName'], temp_dict['jobName'] =  _get_object_name_by_id(hit['_source']['sourceId'])
            temp_dict['objectId'] = hit['_source']['sourceId']
            temp_dict['filePath'] = []
            temp_dict['filePath'].append(hit['_source']['filePath'])
            search_result.append(temp_dict)

    # Write to csv file
    file_name = write_to_csv(search_result)
    df = pd.read_csv(file_name)
    df = df.fillna('')
    df.to_html(file_name.split(".")[0] + ".html")

    files_list = []
    files_list.append(file_name)

    # Send email. There can be multiple recipient.
    send_mail("chandu@cohesity.com", ["cdashudu@gmail.com"], "Insight Search Result Report", "Test", server="smtp.com", port=587, username="", password="", files=files_list)  


# Function to send email
def send_mail(send_from, send_to, subject, message, files=[],
              server="localhost", port=587, username='', password='',
              use_tls=True):
    """Compose and send email with provided info and attachments.

    Args:
        send_from (str): from name
        send_to (list[str]): to name(s)
        subject (str): message title
        message (str): message body
        files (list[str]): list of file paths to be attached to email
        server (str): mail server host name
        port (int): port number
        username (str): server auth username
        password (str): server auth password
        use_tls (bool): use TLS mode
    """
    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = COMMASPACE.join(send_to)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject

    msg.attach(MIMEText(message))

    for path in files:
        f=codecs.open(path.split(".")[0] + '.html', 'r')
        msg.attach(MIMEText(f.read(), 'html'))
        part = MIMEBase('application', "octet-stream")
        with open(path, 'rb') as file:
            part.set_payload(file.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition',
                        'attachment; filename="{}"'.format(Path(path).name))
        msg.attach(part)

    smtp = smtplib.SMTP(server, port)
    if use_tls:
        smtp.starttls()
    smtp.login(username, password)
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.quit()

def write_to_csv(search_result):
    keys = search_result[0].keys()
    date_now = datetime.datetime.now().date().strftime('%m%d%Y')
    file_name ='insight_search_report_' + date_now + '.csv' 
    with open(file_name, 'w', newline='')  as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        for result in search_result:
            dict_writer.writerow({'vmName':result['vmName'], 'sourceName':result['sourceName'], 'jobName':result['jobName'], 'objectId':result['objectId'], 'filePath': ''})    
            for path in result['filePath']:
                dict_writer.writerow({'vmName':'', 'sourceName':'', 'jobName':'', 'objectId':'', 'filePath': path})
        #dict_writer.writerows(search_result){'vnName':result['vmName'], 'sourceName':result['sourceName'], 'jobName':result['jobName'], 'objectId':result['objectId']}
    
    return file_name


def _get_object_name_by_id(protection_object_id):
    r = request('GET', 'public/protectionObjects/summary?protectionSourceId='+ str(protection_object_id))
    protection_object = json.loads(r.content)
    vm_name = protection_object['protectionSource']['name']
    source_name = protection_object['parentProtectionSource']['name']
    protection_job_names = []
    for job in protection_object['protectionJobs']:
        protection_job_names.append(job['jobName'])
    
    return vm_name, source_name, protection_job_names


parser = argparse.ArgumentParser()
parser.add_argument('-s', '--search',  help='String that is to be searched. If both -s and -p are present, -s is used')
parser.add_argument('-p', '--pattern', default='.',  help='Regular expression using Apache Lucene syntax. Default is (.) If both -s and -p are present, -s is used')

if len(sys.argv)==1:
    parser.print_help(sys.stderr)
    sys.exit(1)
args=parser.parse_args()

args = parser.parse_args()
search = args.search
pattern = args.pattern

# Initialize global variables for Cohesity cluster token and App token
token = None
app_token = None
app_nodePort = None
app_session = requests.Session()

### Uncomment this if you want to read cluster details from the user. 
# cluster_endpoint = raw_input("Enter your cluster FQDN or IP : ")
# username = raw_input("Enter your cluster username(admin) : ") or "admin"
# password = raw_input("Enter your cluster password(admin) : ") or "admin"
# org = raw_input("Enter your cluster org name(LOCAL) : ") or "LOCAL"

# Cluster details. Comment below 4 lines if above 4 lines are not commented.
cluster_endpoint = "10.x.x.x"
username = "admin"
password = ""
org = "LOCAL"

# Public API Call URL
cluster_ip_or_fqdn = "https://" + cluster_endpoint +"/irisservices/api/v1/"


get_iris_token(username, 'LOCAL', password) # Get Cohesity Cluster token
get_app_token() # Get App token
get_filepaths(cluster_endpoint, search, pattern) # Get List of filepaths
