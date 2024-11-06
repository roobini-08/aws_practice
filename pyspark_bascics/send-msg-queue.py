import boto3

def send_message_to_sqs(queue_url, message_body):
    # Initialize a boto3 client for SQS
    sqs_client = boto3.client('sqs')
    
    try:
        # Send a message to the specified SQS queue
        response = sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=message_body
        )
        
        # Print the Message ID of the sent message
        message_id = response['MessageId']
        print(f"Message sent successfully! Message ID: {message_id}")
    
    except Exception as e:
        print(f"Error: {e}")

# Replace with your queue URL and message body
queue_url = 'https://sqs.us-east-1.amazonaws.com/593793055422/Queue1'
message_body = 'Hello, this is a test message!'
send_message_to_sqs(queue_url, message_body)
