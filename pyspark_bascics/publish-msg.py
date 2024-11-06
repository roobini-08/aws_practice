#Write a Python script to publish a message to an SNS topic by specifying the topic ARN and a custom message. 
import boto3

def publish_message_to_sns(topic_arn, message):
    # Initialize a boto3 client for SNS
    sns_client = boto3.client('sns')
    
    try:
        # Publish a message to the specified SNS topic
        response = sns_client.publish(
            TopicArn=topic_arn,
            Message=message
        )
        
        # Print the Message ID of the published message
        message_id = response['MessageId']
        print(f"Message published successfully! Message ID: {message_id}")
    
    except Exception as e:
        print(f"Error: {e}")

# Replace with your SNS topic ARN and message
topic_arn = 'arn:aws:sns:us-east-1:593793055422:MySNSTopic'
message = 'Hello, this is a test message for the SNS topic!'
publish_message_to_sns(topic_arn, message)
