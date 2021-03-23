# Package your source code and dependencies in a zip file, upload it to S3, and then use the S3Bucket and S3Keys Properties under your AWS::Lambda::Function resource.
#
#
# mkdir project-dir
# cp myhandler.py project-dir
# pip install module-name -t /path/to/project-dir
#
# # zip the contents of project-dir , this is your deployment package
# cd project-dir
# zip -r deployme

import json
import boto3
import requests
from requests_aws4auth import AWS4Auth
import os
from datetime import datetime as dt
from datetime import timedelta


days_to_keep_online = int(os.environ['DAYS_TO_CHECK'])
host =  os.environ['ES_ENDPOINT']
es_index_pattern_name = os.environ['ES_INDEX_PATTERN_NAME']

region = 'eu-central-1' # For example, us-west-1
service = 'es'

credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)


# def lambda_handler(event, context):

headers = { "Content-Type": "application/json" }

tocheck=es_index_pattern_name+dt.strftime(dt.now() - timedelta(days_to_keep_online), '%Y-%m-%d')
url = host+'/'+tocheck
print(url)
response = requests.get(url, auth=awsauth, headers=headers)
if(response.status_code==200):
    print('Index %s check ',tocheck)
    print('==================')
else:
    print('Error when check ES index %s',tocheck)
    print('==================')
# return(response.json())