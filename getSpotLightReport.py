import codecs
import pandas as pd
import requests
import urllib3
import time
import json
import csv
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


# Generic Function to Make API Calls
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

#Get Apps token and using Cohesity Cluster token and store it in environment variable
def get_app_token():
    r = request('GET', 'public/appInstances')
    global app_token
    global app_nodePort
    all_apps =  json.loads(r.content)
    for app in all_apps:
        # Get Token for Spotlight App. Tokens are different for different apps
        if app['appName'] == "Cohesity Spotlight" and app['healthStatus']=='kHealthy' and app['state']=='kRunning':
            app_token = app['appAccessToken']
            app_nodePort = app['nodePort']
            break

# Main function to create and send report
def download_report(cluster_endpoint, duration):
    # Set Coookie for spotlight app using the below API Call
    r = request('GET', 'https://' + cluster_endpoint + ':' + str(app_nodePort)+ '?token=' + app_token, private_api=True, app_api=True)

    daily_search_filter ={}
    weekly_search_filter ={}
    monthly_search_filter ={}
    # Default Search filter if there are no saved filters present on the Spotlight App in the cluster
    default_search_filter = {
        "view": [],
        "user": [
            "*"
        ],
        "fileType": [],
        "action": [],
        "interval": "day",
        "intervalValue": 40, #Last 40 days of report
        "fileName": []
        }
    download_default = True #If saved filter are not present, use default and download report
    daily_report_name = ""
    weekly_report_name = ""
    monthly_report_name = ""
    default_report_name = ""

    # Get the saved search filters/patterns
    r = request('GET', 'https://' + cluster_endpoint + ':' + str(app_nodePort) + '/api/savedquery/list', private_api=True, app_api=True)

    all_filters = json.loads(r.content)['queries']

    # Read the saved filters and assign it to variables
    for filter in all_filters:
        if 'daily-filter' in filter['name']:
            daily_search_filter = clean_filter(filter['query']['filter'])
            continue
        if 'weekly-filter' in filter['name']:
            weekly_search_filter = clean_filter(filter['query']['filter'])
            continue
        if 'monthly-filter' in filter['name']:
            monthly_search_filter = clean_filter(filter['query']['filter'])
            continue
        
    # Check if saved filters were found and then download the reports.
    if(daily_search_filter and duration == 'daily'):
        download_default = False
        daily_queryId = search_on_filter(daily_search_filter)
        daily_report_name = download_report_on_queryId(daily_queryId, 'daily')
        epoch_to_human_readable(daily_report_name)
    if(weekly_search_filter and duration == 'weekly'):
        download_default = False
        weekly_queryId = search_on_filter(weekly_search_filter)
        weekly_report_name = download_report_on_queryId(weekly_queryId, 'weekly')
        epoch_to_human_readable(weekly_report_name)
    if(monthly_search_filter and duration == 'monthly'):
        download_default = False
        monthly_queryId = search_on_filter(monthly_search_filter)
        monthly_report_name = download_report_on_queryId(monthly_queryId, 'monthly')
        epoch_to_human_readable(monthly_report_name)

    # If saved report is not present, default will be downloaded. Otherwise it will be skipped
    if(download_default):
        default_queryId = search_on_filter(default_search_filter)
        default_report_name = download_report_on_queryId(default_queryId, 'default')
        epoch_to_human_readable(default_report_name)

    # List of files to be emailed
    files_list = []

    if daily_report_name and duration == 'daily':
        files_list.append(daily_report_name)
    if weekly_report_name and duration == 'weekly':
        files_list.append(weekly_report_name)
    if monthly_report_name and duration == 'monthly':
        files_list.append(monthly_report_name)
    if default_report_name:
        files_list.append(default_report_name)

    # Send email. There can be multiple recipient.
    send_mail("chandu@cohesity.com", ["cdashudu@gmail.com"], "Spotlight Report", "Test", server="smtp.com", port=123, username="", password="", files=files_list)  

# Internal Function to convert saved filter params to filter that the API can understand. 
def clean_filter(filter):
    intervalValue = filter['numIntervals']
    interval = filter['duration']
    filter.pop('numIntervals', None)
    filter.pop('duration', None)
    filter.pop('durationValue', None)
    filter.pop('startTime', None)
    filter.pop('endTime', None)
    filter['intervalValue'] = intervalValue
    filter['interval'] = interval
    return filter
    
# API call to download the report
def download_report_on_queryId(queryId, filter_type):
    r = request('GET', 'https://' + cluster_endpoint + ':' + str(app_nodePort)+ '/api/download/report/' + queryId, private_api=True, app_api=True)
    date_now = datetime.datetime.now().date().strftime('%m%d%Y')
    file_name = filter_type +'_spotlight_report_' + date_now + '.csv' 
    with open(file_name, 'wb') as f:
        f.write(r.content)
    return file_name

# API call to search data based on filter. This is an async call where it creates a search and then wait for the response
def search_on_filter(filter):
    r = request('POST', 'https://' + cluster_endpoint + ':' + str(app_nodePort) +'/api/download',data=filter, private_api=True, app_api=True)
    queryId = json.loads(r.content)['queryId']
    query_status_completed = False

    while(not query_status_completed):
        r = request('GET', 'https://' + cluster_endpoint + ':' + str(app_nodePort)+'/api/download/status/' + queryId , private_api=True, app_api=True)
        if (json.loads(r.content)['status'] == "COMPLETED"):
            query_status_completed = True
            return queryId
        else:
            time.sleep(5)
    
    return None

def epoch_to_human_readable(file_name):
    df = pd.read_csv(file_name)
    df['Event Time'] = pd.to_datetime(df['Event Time'],unit='ms').dt.date
    df.to_csv(file_name)
    df.to_html(file_name.split(".")[0] + ".html")

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
    msg = MIMEMultipart('alternative')
    msg['From'] = send_from
    msg['To'] = COMMASPACE.join(send_to)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject

    msg.attach(MIMEText(message))
    #msg.attach(MIMEText())

    

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


parser = argparse.ArgumentParser()
parser.add_argument('-d', '--duration', default='daily', help='Daily, Weekly or Monthly Report. Defaults to (daily)')

args = parser.parse_args()
duration = args.duration

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
password = "admin"
org = "LOCAL"

# Public API Call URL
cluster_ip_or_fqdn = "https://" + cluster_endpoint +"/irisservices/api/v1/"


get_iris_token(username, 'LOCAL', password) # Get Cohesity Cluster token
get_app_token() # Get App token
download_report(cluster_endpoint, duration) # Download and email the report
