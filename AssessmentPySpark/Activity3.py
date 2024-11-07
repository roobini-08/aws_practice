import boto3
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, sum, when
 
# Initialize Spark session
spark = SparkSession.builder \
    .appName("RetailMart_CustomerTierCategorization") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
    .config("spark.hadoop.fs.s3a.access.key", "AKIAYUQGS7K7ELWBNMHX") \
    .config("spark.hadoop.fs.s3a.secret.key", "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") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
customer_table = dynamodb.Table('CustomerDetails')
transaction_table = dynamodb.Table('Transaction_details')

customer_items = customer_table.scan()['Items']
customer_df = spark.createDataFrame([Row(**item) for item in customer_items])
 
# Load transaction data from S3
transaction_df = spark.read.option("header", "true").csv("s3a://data-bktt/transaction_data.csv")
transaction_df.show(5)

# Join customer and transaction data on 'customer_id'
joined_df = customer_df.join(transaction_df, "customer_id", "inner") \
    .select("customer_id", "transaction_id", "amount")
 
# Calculate total spending per customer
total_spending_df = joined_df.groupBy("customer_id") \
    .agg(sum("amount").alias("total_spent"))
 
# Categorize customers based on total spending thresholds
tiered_customers_df = total_spending_df.withColumn(
    "tier",
    when(col("total_spent") < 500, "Bronze")
    .when((col("total_spent") >= 500) & (col("total_spent") < 1000), "Silver")
    .otherwise("Gold")
)
 
tiered_customers_df.show()
 
def update_customer_tier(row):
    customer_table.update_item(
        Key={'customer_id': row['customer_id']},
        UpdateExpression="SET total_spent = :val1, tier = :val2",
        ExpressionAttributeValues={
            ':val1': row['total_spent'],
            ':val2': row['tier']
        }
    )
 
for row in tiered_customers_df.collect():
    update_customer_tier(row)
 

spark.stop()