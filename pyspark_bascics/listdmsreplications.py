#Write a Python program using Boto3 to list all DMS replication instances in your AWS account. 

import boto3

def list_dms_replication_instances():
    # Initialize a boto3 client for DMS
    dms_client = boto3.client('dms')

    # Use describe_replication_instances to get a list of replication instances
    try:
        response = dms_client.describe_replication_instances()
        replication_instances = response.get('ReplicationInstances', [])
        
        # Check if there are any replication instances
        if replication_instances:
            print("DMS Replication Instances:")
            for instance in replication_instances:
                print(f"Instance Identifier: {instance['ReplicationInstanceIdentifier']}")
                print(f"Status: {instance['ReplicationInstanceStatus']}")
                print(f"Instance Class: {instance['ReplicationInstanceClass']}")
                print(f"Engine Version: {instance['EngineVersion']}")
                print(f"Allocated Storage (GB): {instance['AllocatedStorage']}")
                print(f"VPC Security Groups: {[group['VpcSecurityGroupId'] for group in instance['VpcSecurityGroups']]}")
                print("-" * 40)
        else:
            print("No DMS replication instances found.")

    except Exception as e:
        print(f"Error: {e}")

# Call the function
list_dms_replication_instances()
