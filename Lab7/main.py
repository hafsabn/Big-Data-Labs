from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

# Define schema matching your Kafka messages
schema = StructType([
    StructField("title_id", IntegerType()),
    StructField("change_type", StringType()),
    StructField("old_episode_count", IntegerType()),
    StructField("new_episode_count", IntegerType())
])

spark = SparkSession.builder \
    .appName("NetflixChangeTracker") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "netflix_changes") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON from Kafka value
parsed_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Process data - count changes per type
change_counts = parsed_df.groupBy("change_type").count()

# Output to memory with continuous updates
query = change_counts.writeStream \
    .outputMode("complete") \
    .format("memory") \
    .queryName("ChangeCounts") \
    .start()

# Periodically show results
while True:
    print("\n=== Current Change Counts ===")
    spark.sql("SELECT * FROM ChangeCounts").show(truncate=False)
    time.sleep(5)

query.awaitTermination()