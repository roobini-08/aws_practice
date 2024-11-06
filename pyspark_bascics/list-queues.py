#Write a Python program using Boto3 to list all SQS queues in your AWS account and print their URLs. 
import boto3

def list_sqs_queues():
    # Initialize a boto3 client for SQS
    sqs_client = boto3.client('sqs')
    
    try:
        # List all SQS queues
        response = sqs_client.list_queues()
        queue_urls = response.get('QueueUrls', [])

        if queue_urls:
            print("SQS Queues in your AWS account:")
            for url in queue_urls:
                print(url)
        else:
            print("No SQS queues found in your account.")
    
    except Exception as e:
        print(f"Error: {e}")

# Call the function
list_sqs_queues()
