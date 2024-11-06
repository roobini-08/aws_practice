#Write a Python program to list all SNS topics in your AWS account and print their ARNs. 
import boto3

def list_sns_topics():
    # Initialize a boto3 client for SNS
    sns_client = boto3.client('sns')
    
    try:
        # List all SNS topics
        response = sns_client.list_topics()
        topics = response.get('Topics', [])
        
        if topics:
            print("SNS Topics in your AWS account:")
            for topic in topics:
                print(topic['TopicArn'])
        else:
            print("No SNS topics found in your account.")
    
    except Exception as e:
        print(f"Error: {e}")

# Call the function
list_sns_topics()
