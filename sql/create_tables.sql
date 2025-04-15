-- Drop existing table if it exists
DROP TABLE IF EXISTS raw_posts;
DROP TABLE IF EXISTS filtered_posts;
DROP TABLE IF EXISTS sentiment_scores;

-- Table for raw posts
CREATE TABLE raw_posts (
    id TEXT PRIMARY KEY,
    title TEXT,
    selftext TEXT,
    score INTEGER,
    created_utc TIMESTAMP,
    num_comments INTEGER
);

-- Table for filtered posts
CREATE TABLE filtered_posts (
    id TEXT PRIMARY KEY,
    title TEXT,
    selftext TEXT,
    score INTEGER,
    created_utc TIMESTAMP,
    num_comments INTEGER
);

-- Table for sentiment scores
CREATE TABLE sentiment_scores (
    id TEXT PRIMARY KEY,
    title TEXT,
    selftext TEXT,
    score INTEGER,
    created_utc TIMESTAMP,
    num_comments INTEGER,
    sentiment_score TEXT
);
