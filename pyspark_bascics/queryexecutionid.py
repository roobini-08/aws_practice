import boto3

# Initialize the Athena client
athena_client = boto3.client('athena')

# Query to execute
query_string = "SELECT name FROM my_table LIMIT 4;"

# Start the query execution
response = athena_client.start_query_execution(
    QueryString=query_string,
    ResultConfiguration={'OutputLocation': 's3://your-query-results-bucket/'}
)

# Get the QueryExecutionId from the response
query_execution_id = response['QueryExecutionId']
print(f"Query Execution ID: {query_execution_id}")