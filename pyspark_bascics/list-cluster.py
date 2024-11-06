# Write a Python program using Boto3 to list all EMR clusters in your AWS account and print their cluster IDs and statuses. 
import boto3

def list_emr_clusters():
    # Initialize a boto3 client for EMR
    emr_client = boto3.client('emr')
    
    try:
        # List all EMR clusters
        response = emr_client.list_clusters()
        clusters = response.get('Clusters', [])
        
        if clusters:
            print("EMR Clusters in your AWS account:")
            for cluster in clusters:
                cluster_id = cluster['Id']
                cluster_status = cluster['Status']['State']
                print(f"Cluster ID: {cluster_id}, Status: {cluster_status}")
        else:
            print("No EMR clusters found in your account.")
    
    except Exception as e:
        print(f"Error: {e}")

# Call the function
list_emr_clusters()
