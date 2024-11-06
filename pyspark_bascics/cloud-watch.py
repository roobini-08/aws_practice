#Write a Python program to list all CloudWatch alarms in a specific region and print each alarm’s name. 
import boto3

def list_cloudwatch_alarms(region_name):
    # Initialize a boto3 client for CloudWatch in the specified region
    cloudwatch_client = boto3.client('cloudwatch', region_name=region_name)
    
    try:
        # List all CloudWatch alarms
        response = cloudwatch_client.describe_alarms()
        alarms = response.get('MetricAlarms', [])
        
        if alarms:
            print(f"CloudWatch Alarms in region '{region_name}':")
            for alarm in alarms:
                alarm_name = alarm['AlarmName']
                print(alarm_name)
        else:
            print(f"No CloudWatch alarms found in region '{region_name}'.")
    
    except Exception as e:
        print(f"Error: {e}")

# Replace with the specific region you want to query
region_name = 'us-east-1'
list_cloudwatch_alarms(region_name)
