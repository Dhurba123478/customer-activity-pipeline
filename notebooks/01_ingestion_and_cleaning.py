from pyspark.sql.functions import col, to_timestamp

data = [
    (1,101,"M001","play",300,"2024-01-01 10:00:00","mobile"),
    (2,101,"M001","pause",120,"2024-01-01 10:05:00","mobile"),
    (3,102,"M002","play",600,"2024-01-01 11:00:00","tv"),
    (4,103,"M003","play",200,"2024-01-01 11:15:00","web"),
    (5,101,"M001","complete",900,"2024-01-01 11:30:00","mobile"),
    (6,104,"M004","play",0,"2024-01-02 09:00:00","tv"),
    (7,105,"M005","play",450,"2024-01-02 09:15:00","web"),
    (8,102,"M002","complete",700,"2024-01-02 10:00:00","tv"),
    (9,106,None,"play",300,"2024-01-02 10:30:00","mobile"),
    (10,107,"M006","play",-50,"2024-01-02 11:00:00","web")
]

columns = ["event_id","customer_id","content_id","event_type","watch_time_seconds","event_timestamp","device_type"]

df = spark.createDataFrame(data, columns)

df = df.withColumn("event_timestamp", to_timestamp("event_timestamp"))

print("Raw Data")
df.show()

df_clean = df.filter(
    col("customer_id").isNotNull() &
    col("content_id").isNotNull() &
    (col("watch_time_seconds") >= 0)
)

print("Cleaned Data")
df_clean.show()
# Create temp view
df_clean.createOrReplaceTempView("customer_activity_clean")

# Total watch time per customer
spark.sql("""
SELECT customer_id,
       SUM(watch_time_seconds) AS total_watch_time
FROM customer_activity_clean
GROUP BY customer_id
ORDER BY total_watch_time DESC
""").show()

spark.sql("""
SELECT content_id,
       SUM(watch_time_seconds) AS total_watch_time,
       COUNT(*) AS total_events
FROM customer_activity_clean
GROUP BY content_id
ORDER BY total_watch_time DESC
""").show()

spark.sql("""
SELECT DATE(event_timestamp) AS event_date,
       COUNT(DISTINCT customer_id) AS daily_active_users
FROM customer_activity_clean
GROUP BY DATE(event_timestamp)
ORDER BY event_date
""").show()

