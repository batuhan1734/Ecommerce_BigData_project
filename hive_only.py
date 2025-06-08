
# HiveQL Analysis Script - hive_only.py
# This version includes Spark CSV loading and data cleaning steps before writing to Hive

from pyspark.sql.functions import hour, desc, countDistinct

# Load and clean data
df = spark.read.csv("gs://ecommerce-data-bucket-test/2019-Oct.csv", header=True, inferSchema=True)
df_clean = df.na.drop()

# Save cleaned DataFrame as Hive Table
df_clean.write.mode("overwrite").format("parquet").saveAsTable("ecommerce_oct2019")

# 1. Count of each event type
spark.sql("SELECT event_type, COUNT(*) AS total FROM ecommerce_oct2019 GROUP BY event_type").show()

# 2. Hourly distribution of purchases
spark.sql("""
SELECT HOUR(event_time) AS hour, COUNT(*) AS purchases
FROM ecommerce_oct2019
WHERE event_type = 'purchase'
GROUP BY hour
ORDER BY hour
""").show()

# 3. Top 10 most purchased products
spark.sql("""
SELECT product_id, COUNT(*) AS purchase_count
FROM ecommerce_oct2019
WHERE event_type = 'purchase'
GROUP BY product_id
ORDER BY purchase_count DESC
LIMIT 10
""").show()

# 4. Total revenue by product
spark.sql("""
SELECT product_id, SUM(price) AS total_revenue
FROM ecommerce_oct2019
WHERE event_type = 'purchase'
GROUP BY product_id
ORDER BY total_revenue DESC
LIMIT 10
""").show()

# 5. Unique users by event type
spark.sql("""
SELECT event_type, COUNT(DISTINCT user_id) AS unique_users
FROM ecommerce_oct2019
GROUP BY event_type
""").show()
