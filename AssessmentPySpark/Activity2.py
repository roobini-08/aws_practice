import boto3
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col, count
 
# Initialize Spark session
spark = SparkSession.builder.appName("RetailMart_LoyaltyTracking") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
    .config("spark.hadoop.fs.s3a.access.key", "AKIAYUQGS7K7ELWBNMHX") \
    .config("spark.hadoop.fs.s3a.secret.key", "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") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()
 

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('CustomerDetails')
 
items = table.scan()['Items']
customer_df = spark.createDataFrame([Row(**item) for item in items])
 
# Load transaction data from S3
transaction_df = spark.read.option("header", "true").csv("s3a://data-bktt/transaction_data.csv")
transaction_df.show(5)

joined_df = customer_df.join(transaction_df, "customer_id", "inner").select("customer_id", "transaction_id")
joined_df.show(5)
 
# Count repeat purchases per customer
repeat_purchases_df = joined_df.groupBy("customer_id").agg(count("transaction_id").alias("purchase_count"))
 
# Filter customers with more than one purchase (repeat customers)
repeat_customers_df = repeat_purchases_df.filter(col("purchase_count") > 1)
 
top_repeat_customers_df = repeat_customers_df.orderBy(col("purchase_count").desc())
top_repeat_customers_df.show()
spark.stop()