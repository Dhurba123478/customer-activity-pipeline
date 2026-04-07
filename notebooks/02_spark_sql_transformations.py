from pyspark.sql.functions import col, to_timestamp

# Read raw customer activity data
df = spark.read.option("header", True).csv("/FileStore/customer_activity.csv")

# Convert columns to proper data types
df = (
    df.withColumn("event_id", col("event_id").cast("int"))
      .withColumn("customer_id", col("customer_id").cast("int"))
      .withColumn("watch_time_seconds", col("watch_time_seconds").cast("int"))
      .withColumn("event_timestamp", to_timestamp("event_timestamp"))
)

# Clean data
df_clean = df.filter(
    col("customer_id").isNotNull() &
    col("content_id").isNotNull() &
    col("watch_time_seconds").isNotNull() &
    (col("watch_time_seconds") >= 0)
)

# Create temp view for Spark SQL
df_clean.createOrReplaceTempView("customer_activity_clean")

# 1. Total watch time by customer
customer_watch_time = spark.sql("""
SELECT
    customer_id,
    SUM(watch_time_seconds) AS total_watch_time,
    COUNT(*) AS total_events
FROM customer_activity_clean
GROUP BY customer_id
ORDER BY total_watch_time DESC
""")

print("Total watch time by customer:")
customer_watch_time.show()

# 2. Daily active users
daily_active_users = spark.sql("""
SELECT
    DATE(event_timestamp) AS event_date,
    COUNT(DISTINCT customer_id) AS daily_active_users
FROM customer_activity_clean
GROUP BY DATE(event_timestamp)
ORDER BY event_date
""")

print("Daily active users:")
daily_active_users.show()

# 3. Top content by engagement
top_content = spark.sql("""
SELECT
    content_id,
    COUNT(*) AS engagement_count,
    SUM(watch_time_seconds) AS total_watch_time
FROM customer_activity_clean
GROUP BY content_id
ORDER BY total_watch_time DESC
""")

print("Top content by engagement:")
top_content.show()
