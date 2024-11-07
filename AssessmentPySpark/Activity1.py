# As a Data Engineer at RetailMart, you’re tasked with performing a comprehensive Customer Sales Analysis to
# provide insights into purchasing patterns and product performance. First, you initialize a Spark session named
# "CustomerAnalysis" to handle large datasets. Then, you load order data from a CSV file stored in S3 and display the
# initial rows to verify the data. Focusing on high-value purchases, you filter orders with an amount over ₹1,000,
# then add a discounted_price column reflecting a 10% discount on the original price. Next, you group the sales data
#  by product_category to calculate total sales per category, offering insights into the top-selling products.
# To build a complete customer view, you join the customer and order DataFrames on customer_id. Additionally,
# you analyze employee tenure by adding a years_of_experience column based on joining_date in the employee DataFrame.
# Finally, you save the cleaned and aggregated sales data back to S3 in Parquet format for efficient storage
# and future analysis, equipping RetailMart with actionable insights for strategic decision-making.
 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff, current_date
 
spark = SparkSession.builder.appName("CustomerAnalysis") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901") \
    .config("spark.hadoop.fs.s3a.access.key", "AKIAYUQGS7K7ELWBNMHX") \
    .config("spark.hadoop.fs.s3a.secret.key", "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") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.path.style.access", "false") \
    .config("spark.hadoop.fs.s3a.metastore.metrics.enabled", "false") \
    .config("spark.hadoop.io.native.lib.available", "false") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()
 
#Load Data from S3
order_data_path = "s3a://data-bktt/order_data.csv"
orders_df = spark.read.csv(order_data_path, header=True, inferSchema=True)
orders_df.show(5)
 
high_value_orders_df = orders_df.filter(orders_df.amount > 1000)

high_value_orders_df = high_value_orders_df.withColumn(
    "discounted_price", col("amount") * 0.90
)
high_value_orders_df.show(5)
 
#Group Sales by Product Category
sales_by_category_df = high_value_orders_df.groupBy("product_category") \
    .agg(
        {"discounted_price": "sum"}  
    ) \
    .withColumnRenamed("sum(discounted_price)", "total_sales")
sales_by_category_df.show()
 
#Join with Customer Data to Build a Complete Customer View
customer_data_path = "s3a://data-bktt/customer_data.csv"
customers_df = spark.read.csv(customer_data_path, header=True, inferSchema=True)
 
# Join the customer DataFrame with the high-value orders DataFrame
customer_sales_df = high_value_orders_df.join(
    customers_df, on="customer_id", how="inner"
)
 
customer_sales_df.show(5)
 
employee_data_path = "s3a://data-bktt/employee_data.csv"
employees_df = spark.read.csv(employee_data_path, header=True, inferSchema=True)
 
# Calculate years of experience based on joining_date
employees_df = employees_df.withColumn(
    "years_of_experience", datediff(current_date(), col("joining_date")) / 365
)

employees_df.show(5)
 
# Save the Cleaned and Aggregated Sales Data in Parquet Format
sales_by_category_df.write.parquet("s3a://op-bkt/sales_by_category.parquet")
customer_sales_df.write.parquet("s3a://op-bkt/customer_sales.parquet")