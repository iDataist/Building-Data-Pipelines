import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= "CREATE TABLE IF NOT EXISTS staging_events (name VARCHAR,	auth VARCHAR, first_name VARCHAR, gender VARCHAR, itemInSession INT, last_name VARCHAR, length NUMERIC,level VARCHAR, location VARCHAR,	method VARCHAR,	page VARCHAR, registration INT, sessionId INT, title VARCHAR, status INT, ts BIGINT, user_agent VARCHAR, user_id INT);"

staging_songs_table_create = "CREATE TABLE IF NOT EXISTS staging_songs (artist_id VARCHAR, lattitude NUMERIC, location VARCHAR,longitude NUMERIC, name VARCHAR, duration NUMERIC, num_songs INT, song_id VARCHAR, title VARCHAR, year INT);"

songplay_table_create = "CREATE TABLE IF NOT EXISTS songplays (songplay_id INT IDENTITY(0,1) PRIMARY KEY, start_time TIMESTAMP, user_id INT, level VARCHAR, song_id VARCHAR, artist_id VARCHAR, session_id INT, location VARCHAR, user_agent VARCHAR);"

user_table_create = "CREATE TABLE IF NOT EXISTS users (user_id INT PRIMARY KEY, first_name VARCHAR, last_name VARCHAR, gender VARCHAR, level VARCHAR);"

song_table_create = "CREATE TABLE IF NOT EXISTS songs (song_id VARCHAR PRIMARY KEY, title VARCHAR, artist_id VARCHAR, year INT, duration NUMERIC);"

artist_table_create = "CREATE TABLE IF NOT EXISTS artists (artist_id VARCHAR PRIMARY KEY, name VARCHAR, location VARCHAR, lattitude NUMERIC, longitude NUMERIC);"

#time datatype?
time_table_create = "CREATE TABLE IF NOT EXISTS time (start_time TIMESTAMP, hour INT, day INT, week INT, month INT, year INT, weekday VARCHAR);"

staging_events_copy = ("""
copy staging_events from {}
credentials 'aws_iam_role={}'
json{};
""").format(config['S3']['LOG_DATA'] , config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
copy staging_events from {}
credentials 'aws_iam_role={}'
json'auto';
""").format(config['S3']['SONG_DATA'] , config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = "INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT
DATEADD(MILLISECOND,ts,'1970-1-1')[time] AS start_time,
userId AS user_id,
level,
song_id,
artist_id,
sessionId AS session_id,
location,
userAgent AS user_agent
FROM staging_songs s
JOIN staging_events e on s.title = e.song"

user_table_insert = "INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT
userId AS user_id,
firstName AS first_name,
lastName AS last_name,
gender,
level
FROM staging_events"

song_table_insert = "INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT
song_id,
title,
artist_id,
year,
duration
FROM staging_songs"

artist_table_insert = "INSERT INTO artists (artist_id, name, location, lattitude, longitude)
SELECT
artist_id,
artist_name AS name,
artist_location AS location,
artist_latitude AS lattitude,
artist_longitude AS longitude
FROM staging_songs"

time_table_insert = "INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT
AS start_time,
AS hour,
AS day,
AS week,
AS month,
AS year,
AS weekday
AS FROM staging_events"

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
