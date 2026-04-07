-- Total watch time per customer
SELECT customer_id,
       SUM(watch_time_seconds) AS total_watch_time
FROM customer_activity_clean
GROUP BY customer_id
ORDER BY total_watch_time DESC;

-- Top content
SELECT content_id,
       SUM(watch_time_seconds) AS total_watch_time
FROM customer_activity_clean
GROUP BY content_id
ORDER BY total_watch_time DESC;

-- Daily active users
SELECT DATE(event_timestamp) AS event_date,
       COUNT(DISTINCT customer_id) AS daily_active_users
FROM customer_activity_clean
GROUP BY DATE(event_timestamp)
ORDER BY event_date;
