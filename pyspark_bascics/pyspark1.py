#Create a PySpark program to read CSV files from multiple folders in an S3 bucket, 
# combine them into a single DataFrame, and store the result in a new S3 folder as Parquet.
 
from pyspark.sql import SparkSession
spark = SparkSession.builder \
  .appName("CombineCSVFiles") \
   .getOrCreate()
input_path = "s3://demo-buckk/path/to/csv-files/"
output_path = "s3://your-bucket/path/to/parquet-files/"  # Replace with your desired S3 output path
df = spark.read.csv(input_path, header=True, inferSchema=True)
df.write.mode("overwrite").parquet(output_path)
spark.stop()
 
