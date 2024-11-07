import boto3
from pyspark.sql import Row
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType
 
# Initialize Spark session
spark = SparkSession.builder \
    .appName("CustomerEngagementAnalysis") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
    .config("spark.hadoop.fs.s3a.access.key", "AKIAYUQGS7K7ELWBNMHX") \
    .config("spark.hadoop.fs.s3a.secret.key", "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") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()
 
# Initialize Boto3 DynamoDB client
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
customer_table = dynamodb.Table('CustomerDetails')
 
# Load customer data from DynamoDB
customer_items = customer_table.scan()['Items']
customer_df = spark.createDataFrame([Row(**item) for item in customer_items])
 
# Load transaction data from S3 (ensure the CSV file includes 'customer_id' and 'transaction_date' columns)
transaction_df = spark.read.option("header", "true").csv("s3a://data-bktt/transaction_data.csv")
transaction_df.show(5)

transaction_df = transaction_df.withColumn("timestamp", F.to_timestamp("transaction_date", "dd-MM-yyyy"))
 

joined_df = transaction_df.join(customer_df, "customer_id", "inner")
 
# Sort transactions by 'customer_id' and 'timestamp' to calculate the time difference
joined_df = joined_df.orderBy("customer_id", "timestamp")
 

window_spec = Window.partitionBy("customer_id").orderBy("timestamp")
joined_df = joined_df.withColumn("prev_timestamp", F.lag("timestamp").over(window_spec))
 
# Calculate the time difference in seconds
joined_df = joined_df.withColumn("time_diff", F.unix_timestamp("timestamp") - F.unix_timestamp("prev_timestamp"))
 
# Filter out rows with null time_diff (first purchase for each customer will have null)
joined_df = joined_df.filter(joined_df.time_diff.isNotNull())
 
# Calculate average transaction interval for each customer
avg_interval_df = joined_df.groupBy("customer_id").agg(F.avg("time_diff").alias("avg_transaction_interval"))
 
# Convert time difference from seconds to a more readable format (e.g., days)
avg_interval_df = avg_interval_df.withColumn("avg_transaction_interval_days", avg_interval_df["avg_transaction_interval"] / (60 * 60 * 24))
 
avg_interval_df.show()

high_engagement_customers_df = avg_interval_df.filter(avg_interval_df["avg_transaction_interval_days"] < 7)
high_engagement_customers_df.show()
spark.stop()
