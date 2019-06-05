import boto3
import socket
from botocore.exceptions import ClientError
from collections import defaultdict

VPC_ID = "vpc-84fe3aed"
REGION = "eu-central-1"
JG_NAME_PREFIX = "jg-gremlin"
JG_PORT = 8182

def getJgInstances():
    items = defaultdict()
    vpc = boto3.resource('ec2').Vpc(VPC_ID)
    for instance in vpc.instances.all():
        #print(instance.instance_id)
        if (instance.tags != None):
            for tag in instance.tags:
                if tag['Key'] == 'Name':
                    i_name = tag['Value']
                    # print(i_name)
                    if JG_NAME_PREFIX in i_name:
                        items[instance.instance_id] = {
                            'Name': i_name,
                            'State': instance.state['Name'],
                            'Launch Time': instance.launch_time,
                            'Private IP': instance.private_ip_address,
                            'Public IP': instance.public_ip_address
                        }
    return items


def isJgInstanceAlive(instance_id, ip):
    canConnect = True
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((ip, JG_PORT))
    except socket.error as e:
        canConnect = False
    return canConnect


def rebootJgInstance(instance_id):
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


attributes = ['Name', 'State', 'Private IP', 'Public IP', 'Launch Time']
jgInstances = getJgInstances()
for instance_id, instance in jgInstances.items():
    for key in attributes:
        print("{0}: {1}".format(key, instance[key]))
    alive = isJgInstanceAlive(instance_id, instance['Public IP'])
    print("JG Alive: {0}".format(alive))
    if(not alive):
        rebootJgInstance(instance_id)
    print("------")