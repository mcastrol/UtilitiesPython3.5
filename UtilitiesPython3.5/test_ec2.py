import json
import boto3
from botocore.exceptions import ClientError


#ec2 = boto3.client('ec2')

# get list of ec2 instances

# try:
#     ec2 = boto3.client('ec2')
#     response = ec2.describe_instances()
#     print(response)
# except Exception as e:
#     print("describe instance fails")



#describe all the environments
# try:
#     client = boto3.client('elasticbeanstalk')
#     response = client.describe_environments()
#     # print(response)
#     print(response['ResponseMetadata']['HTTPHeaders'])
#     print(response['Environments'])
#
# except Exception as e:
#     print("describe environments")



#describe some environments
try:
    client = boto3.client('elasticbeanstalk')
    environmentNames=['admin-staging-test']
    response = client.describe_environments(EnvironmentNames=environmentNames)
    print(response)
except Exception as e:
    print("describe environments")


try:
    client = boto3.client('elasticbeanstalk')
    environmentNames=['admin-staging-test']
    response = client.terminate_environment(EnvironmentName=environmentNames[0])
    print(response)

except Exception as e:
    print("terminate error ")
    print(e)

