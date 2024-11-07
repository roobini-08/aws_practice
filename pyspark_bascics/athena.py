import boto3

import time
 
try:

    client = boto3.client('athena', region_name='us-east-1')  

except Exception as e:

    print("Error initializing Athena client:", e)

    exit()

try:

    response = client.start_query_execution(

        QueryString="SHOW DATABASES",

        ResultConfiguration={'OutputLocation': 's3://demo-buckk/folder/'}      )

    query_id = response['QueryExecutionId']

    print("Query started successfully with ID:", query_id)

except Exception as e:

    print("Error starting Athena query:", e)

    exit()

try:

    while True:

        # Check the status of the query

        status = client.get_query_execution(QueryExecutionId=query_id)

        state = status['QueryExecution']['Status']['State']        

        if state == 'SUCCEEDED':

            print("Query succeeded!")

            break

        elif state in ['FAILED', 'CANCELLED']:

            print("Query failed or was cancelled with state:", state)

            exit()

        time.sleep(1)

except Exception as e:

    print("Error checking query status:", e)

    exit()

try:

    results = client.get_query_results(QueryExecutionId=query_id)

    for row in results['ResultSet']['Rows'][1:]:

        print(row['Data'][0]['VarCharValue'])

except Exception as e:

    print("Error fetching query results:", e)

    exit()

 