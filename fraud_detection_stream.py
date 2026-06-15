"""
fraud_detection_stream.py  --  ENGR 5785G Real-Time Stream Processing (Scenario C)
E-Commerce Fraud Detection with Spark Structured Streaming + SESSION windows.

Pipeline:
  readStream (watched directory of CSV files)
    -> withWatermark on event_time
    -> groupBy( session_window(event_time, 30 min), user_id )   [STATEFUL]
    -> aggregate cart / purchase / total events per session
    -> FILTER alert: carts > 5 AND purchases == 0  (cart-without-purchase)
    -> writeStream to console (append mode) = the alert output stream

Satisfies the requirements:
  * readStream with a watched directory
  * a window aggregation (session_window) with withWatermark
  * an alert condition delivered as a separate filtered output stream
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, session_window, sum as _sum, count, when, lit
from pyspark.sql.types import (StructType, StructField, StringType,
                               LongType, DoubleType, TimestampType)

INPUT_DIR = "input_stream"
SESSION_GAP = "30 minutes"     # inactivity gap that ends a shopping session
WATERMARK   = "10 minutes"     # how long we wait for late events
CART_THRESHOLD = 5             # alert when carts strictly exceed this

# The streaming file source needs an explicit schema (it cannot infer it safely).
SCHEMA = StructType([
    StructField("event_time",    TimestampType(), True),
    StructField("event_type",    StringType(),    True),
    StructField("product_id",    LongType(),      True),
    StructField("category_id",   LongType(),      True),
    StructField("category_code", StringType(),    True),
    StructField("brand",         StringType(),    True),
    StructField("price",         DoubleType(),    True),
    StructField("user_id",       LongType(),      True),
    StructField("user_session",  StringType(),    True),
])

def main():
    spark = (SparkSession.builder
             .appName("EcommerceFraudSessionWindow")
             .master("local[2]")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.sql.streaming.schemaInference", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # 1) SOURCE: watch a directory; each new CSV file is a micro-batch.
    events = (spark.readStream
              .format("csv")
              .option("header", "true")
              .option("maxFilesPerTrigger", 1)     # one file per trigger = stream-like
              .schema(SCHEMA)
              .load(INPUT_DIR))

    # 2) STATEFUL WINDOW AGGREGATION: per-user session window + watermark.
    sessions = (events
                .withWatermark("event_time", WATERMARK)
                .groupBy(session_window(col("event_time"), SESSION_GAP), col("user_id"))
                .agg(
                    _sum(when(col("event_type") == "cart",     1).otherwise(0)).alias("num_cart"),
                    _sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("num_purchase"),
                    count("*").alias("num_events"),
                ))

    # 3) ALERT (filtered output stream): >5 carts and no purchase in the session.
    alerts = (sessions
              .filter((col("num_cart") > CART_THRESHOLD) & (col("num_purchase") == 0))
              .select(
                  lit("FRAUD_FLAG: cart-without-purchase").alias("alert"),
                  col("user_id"),
                  col("session_window.start").alias("session_start"),
                  col("session_window.end").alias("session_end"),
                  col("num_cart"),
                  col("num_purchase"),
              ))

    # 4) SINK: stream the alerts to the console (append mode for session windows).
    query = (alerts.writeStream
             .outputMode("append")
             .format("console")
             .option("truncate", "false")
             .option("numRows", 50)
             .trigger(processingTime="2 seconds")
             .start())

    print(">>> Streaming started. Watching ./input_stream for new files. Alerts below:\n")
    query.awaitTermination()

if __name__ == "__main__":
    main()
