from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, from_unixtime, when
from pyspark.sql.types import *
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# Initialize Sentiment Analyzer
analyzer = SentimentIntensityAnalyzer()

# Define UDF for sentiment analysis
def get_sentiment(text):
    if not text:
        return "neutral"
    score = analyzer.polarity_scores(text)
    compound = score.get('compound', 0)
    if compound >= 0.05:
        return "positive"
    elif compound <= -0.05:
        return "negative"
    else:
        return "neutral"

sentiment_udf = udf(get_sentiment, StringType())

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SentimentScoresConsumer") \
    .config("spark.jars", "/home/sravani/jars/postgresql-42.3.1.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema for JSON structure
schema = StructType([
    StructField("id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("selftext", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("created_utc", DoubleType(), True),
    StructField("num_comments", IntegerType(), True)
])

# Read from Kafka topic
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "reddit-sentiment-scores") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON and convert fields
json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Handle missing selftext and convert timestamp
json_df = json_df.withColumn(
    "selftext",
    when(col("selftext").isNull() | (col("selftext") == ""), "No content").otherwise(col("selftext"))
)

# Apply sentiment analysis and convert epoch to timestamp
with_sentiment = json_df.withColumn("sentiment_score", sentiment_udf(col("title"))) \
    .withColumn("created_utc", from_unixtime(col("created_utc")).cast(TimestampType()))

# Function to write microbatches to Postgres
def write_to_postgres(df, epoch_id):
    df.write.format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/reddit_stream_db") \
        .option("dbtable", "sentiment_scores") \
        .option("user", "root") \
        .option("password", "root") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

# Start streaming query
query = with_sentiment.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

query.awaitTermination()
