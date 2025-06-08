
# PySpark Analysis - spark_only.py

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, col, desc, countDistinct

# Create Spark session 
spark = SparkSession.builder.appName("EcommerceSparkAnalysis").getOrCreate()

# Load CSV file from Google Cloud Storage
df = spark.read.csv("gs://ecommerce-data-bucket-test/2019-Oct.csv", header=True, inferSchema=True)

# Drop rows with null values
df_clean = df.na.drop()

# Show basic schema
df_clean.printSchema()

# 1. Count of each event type
df_clean.groupBy("event_type").count().show()

# 2. Hourly distribution of purchases
df_clean = df_clean.withColumn("hour", hour("event_time"))
df_clean.filter(df_clean.event_type == "purchase") \
    .groupBy("hour").count().orderBy("hour").show()

# 3. Top 10 most purchased products
df_clean.filter(df_clean.event_type == "purchase") \
    .groupBy("product_id").count().orderBy("count", ascending=False).show(10)

# 4. Total revenue per product (price * number of purchases)
df_clean.filter(df_clean.event_type == "purchase") \
    .groupBy("product_id") \
    .agg({'price': 'sum'}) \
    .withColumnRenamed("sum(price)", "total_revenue") \
    .orderBy(desc("total_revenue")).show(10)

# 5. Number of unique users per event type
df_clean.groupBy("event_type").agg(countDistinct("user_id").alias("unique_users")).show()
