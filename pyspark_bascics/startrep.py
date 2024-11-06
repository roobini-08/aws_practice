import boto3

def start_dms_replication_task(task_arn):
    # Initialize a boto3 client for DMS
    dms_client = boto3.client('dms')

    try:
        # Start the replication task
        response = dms_client.start_replication_task(
            ReplicationTaskArn=task_arn,
            StartReplicationTaskType='start-replication'  # Choose start-replication to start a new task run
        )
        
        # Print status information
        status = response['ReplicationTask']['Status']
        print(f"Replication task '{task_arn}' started successfully with status: {status}")

    except Exception as e:
        print(f"Error: {e}")

# Replace 'your-task-arn' with the actual ARN of the replication task you want to start
task_arn = 'arn:aws:dms:us-east-1:593793055422:rep:YLNX7MLKBVD2RIVUCOFJLFMSGA'
start_dms_replication_task(task_arn)
