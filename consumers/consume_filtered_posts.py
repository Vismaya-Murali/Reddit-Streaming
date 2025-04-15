from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime
from pyspark.sql.types import *

# Initialize Spark session
spark = SparkSession.builder \
    .appName("FilteredPostsConsumer") \
    .config("spark.jars", "/home/sravani/jars/postgresql-42.3.1.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema for JSON
schema = StructType([
    StructField("id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("selftext", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("created_utc", DoubleType(), True),
    StructField("num_comments", IntegerType(), True)
])

# Read from Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "reddit-filtered-posts") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON and convert created_utc
json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Convert created_utc (epoch) to timestamp
json_df = json_df.withColumn("created_utc", from_unixtime(col("created_utc")).cast(TimestampType()))

# Function to write each microbatch to Postgres
def write_to_postgres(df, epoch_id):
    df.write.format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/reddit_stream_db") \
        .option("dbtable", "filtered_posts") \
        .option("user", "root") \
        .option("password", "root") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

# Start streaming query
query = json_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

query.awaitTermination()
