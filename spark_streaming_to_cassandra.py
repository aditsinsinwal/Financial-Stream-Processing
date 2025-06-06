"""
spark_streaming_to_cassandra.py
PySpark structured streaming job that reads json messages from Kafka, 
  parses out fields and computes simpled derived metrics
  and writes enriched dataframe into cassandra in real time.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, current_timestamp, window, avg, expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, TimestampType
)

# Read environment / default config
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock-quotes")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "")
CASSANDRA_PORT = os.getenv("CASSANDRA_PORT", "")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "market_data")
CASSANDRA_TABLE = os.getenv("CASSANDRA_TABLE", "quotes")

def create_spark_session():
    """
    Build a SparkSession configured with the Cassandra connector.
    """
    return (
        SparkSession.builder
        .appName("KafkaToCassandraStreaming")
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .config("spark.cassandra.connection.port", CASSANDRA_PORT)
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,"
                "com.datastax.spark:spark-cassandra-connector_2.12:3.3.0")
        .getOrCreate()
    )

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    quote_schema = StructType([
        StructField("c", DoubleType(), True),            # current price
        StructField("h", DoubleType(), True),            # high price for the day
        StructField("l", DoubleType(), True),            # low price for the day
        StructField("o", DoubleType(), True),            # open price of the day
        StructField("pc", DoubleType(), True),           # prev close
        StructField("t", LongType(), True),              # Finnhub timestamp
        StructField("symbol", StringType(), True),       # ticker
        StructField("fetched_at", LongType(), True)      # our local timestamp
    ])

    # 1) Reading from Kafka
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")  # or "earliest"
        .load()
    )

    # The Kafka "value" is bytes; convert to string then parse JSON
    json_df = raw_df.selectExpr("CAST(value AS STRING) as json_str")

    parsed_df = json_df.select(
        from_json(col("json_str"), quote_schema).alias("data")
    ).select("data.*")

    # 2) Deriving additional metrics
    #     example, compute a simple 1-minute sliding average of "c" (current price)
    #     and also convert Finnhub's timestamp into a proper TimestampType.
    df_with_ts = parsed_df.withColumn(
        "event_time",
        (col("t").cast("double") * 1000).cast("timestamp")
    ).withColumn(
        "processing_time", current_timestamp()
    )

    # Example aggregation: 1-minute sliding window average price per symbol
    # This will produce rows every 30 seconds
    windowed_avg = (
        df_with_ts
        .withWatermark("event_time", "2 minutes")
        .groupBy(
            col("symbol"),
            window(col("event_time"), "1 minute", "30 seconds")
        )
        .agg(
            avg("c").alias("avg_price_1min")
        )
    ).select(
        col("symbol"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("avg_price_1min")
    )

    # 3) Writing enriched stream to Cassandra
    #    We want each raw quote + the 1-min average (joined back) OR write separately.
    #    For demonstration, let's write raw quotes to one table and windowed_avg to another.

    # 3a) Write raw quotes table:
    raw_to_cassandra = (
        df_with_ts
        .select(
            col("symbol"),
            col("event_time"),
            col("processing_time"),
            col("c").alias("current_price"),
            col("h").alias("high_price"),
            col("l").alias("low_price"),
            col("o").alias("open_price"),
            col("pc").alias("prev_close"),
            col("fetched_at")
        )
        .writeStream
        .format("org.apache.spark.sql.cassandra")
        .option("keyspace", CASSANDRA_KEYSPACE)
        .option("table", CASSANDRA_TABLE)
        .outputMode("append")
        .option("checkpointLocation", "/tmp/checkpoints/quotes_raw")
        .start()
    )

    # 3b) Writing windowed averages to a separate table:
    avg_to_cassandra = (
        windowed_avg
        .select(
            col("symbol"),
            col("window_start"),
            col("window_end"),
            col("avg_price_1min")
        )
        .writeStream
        .format("org.apache.spark.sql.cassandra")
        .option("keyspace", CASSANDRA_KEYSPACE)
        .option("table", f"{CASSANDRA_TABLE}_1min_avg")
        .outputMode("update")  # Because window aggregates get updated
        .option("checkpointLocation", "/tmp/checkpoints/quotes_1min_avg")
        .start()
    )

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
