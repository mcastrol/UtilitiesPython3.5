#verify https://shopify-app.shoesize.me/public/index.html
#https://shopify-app-staging.shoesize.me/public/index.html
import boto3
import requests
import os
from botocore.exceptions import ClientError
from collections import defaultdict


VPC_ID = "vpc-84fe3aed"
REGION = "eu-central-1"

def getShopifyInstances(instance_prefix_name):
    items = defaultdict()
    vpc = boto3.resource('ec2').Vpc(VPC_ID)
    for instance in vpc.instances.all():
        #print(instance.instance_id)
        if (instance.tags != None):
            for tag in instance.tags:
                if tag['Key'] == 'Name':
                    i_name = tag['Value']
                    # print(i_name)
                    if instance_prefix_name in i_name:
                        items[instance.instance_id] = {
                            'Name': i_name,
                            'State': instance.state['Name'],
                            'Launch Time': instance.launch_time,
                            'Private IP': instance.private_ip_address,
                            'Public IP': instance.public_ip_address
                        }
    return items



def check(url):
    r = requests.get(url)
    print(url)
    print(r)
    return str(r.status_code)



def rebootShopifyInstance(instance_id):
    ec2 = boto3.client('ec2', REGION)
    try:
        ec2.reboot_instances(InstanceIds=[instance_id], DryRun=True)
    except ClientError as e:
        if 'DryRunOperation' not in str(e):
            print("Reboot Failure: You don't have permission to reboot instances.")
            # raise
            return

    try:
        response = ec2.reboot_instances(InstanceIds=[instance_id], DryRun=False)
        print('Reboot Success: ', response)
    except ClientError as e:
        print('Error', e)

def lambda_handler(event, context):
    attributes = ['Name', 'State', 'Private IP', 'Public IP', 'Launch Time']
    url = os.environ['URL']
    response_code = os.environ['RESPONSE_CODE']
    instance_prefix_name=os.environ['INSTANCE_PREFIX_NAME']

    if check(url) != response_code:
        print("{url} is down - to reboot the instances".format(url=url))
        shopifyInstances = getShopifyInstances(instance_prefix_name)
        print(shopifyInstances)
        for instance_id, instance in shopifyInstances.items():
            for key in attributes:
                print("{0}: {1}".format(key, instance[key]))
            rebootShopifyInstance(instance_id)
        print("------")
    else:
        print("{url} is up".format(url=url))




