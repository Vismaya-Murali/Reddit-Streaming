import praw
import json
from kafka import KafkaProducer
from textblob import TextBlob
import time

# Setup Reddit API
reddit = praw.Reddit(
    client_id='WNlvBYoiez6dSy1U7L2RJA',
    client_secret='npfshKDZIhPYnvkGZ3xgECxCTog60Q',
    user_agent='python:reddit_streamer:v1.0 (by /u/sravz_24)'
)

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Access subreddit
subreddit = reddit.subreddit('PESU')

# Fetch posts
for post in subreddit.new(limit=None):
    data = {
        'id': post.id,
        'title': post.title,
        'score': post.score,
        'created_utc': post.created_utc,
        'num_comments': post.num_comments,
        'selftext': post.selftext if post.selftext else ""
    }

    # Send to 'reddit-raw-posts'
    producer.send('reddit-raw-posts', value=data)

    # If score > 10, send to 'reddit-filtered-posts'
    if post.score > 10:
        producer.send('reddit-filtered-posts', value=data)

    # Calculate sentiment
    sentiment_score = TextBlob(data['selftext']).sentiment.polarity
    sentiment_data = {
        'id': post.id,
        'title': post.title,
        'sentiment_score': sentiment_score
    }

    # Send to 'reddit-sentiment-scores'
    producer.send('reddit-sentiment-scores', value=sentiment_data)

    print(f"Produced to topics: {data['id']}")
    time.sleep(1)

producer.flush()
producer.close()
