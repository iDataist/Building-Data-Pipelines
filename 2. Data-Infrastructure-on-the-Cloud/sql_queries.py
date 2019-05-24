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

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events 
(artist VARCHAR,	
auth VARCHAR, 
firstName VARCHAR, 
gender VARCHAR, 
itemInSession INT, 
lastName VARCHAR, 
length NUMERIC,level VARCHAR, 
location VARCHAR,	
method VARCHAR,	
page VARCHAR, 
registration NUMERIC, 
sessionId INT, 
song VARCHAR, 
status INT, 
ts BIGINT, 
userAgent VARCHAR, 
userId INT);""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs 
(artist_id VARCHAR, 
artist_latitude NUMERIC, 
artist_location VARCHAR,
artist_longitude NUMERIC, 
artist_name VARCHAR, 
duration NUMERIC, 
num_songs INT, 
song_id VARCHAR, 
title VARCHAR, 
year INT);""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays 
(songplay_id INT IDENTITY(0,1) 
PRIMARY KEY, 
start_time TIMESTAMP, 
user_id INT, 
level VARCHAR, 
song_id VARCHAR, 
artist_id VARCHAR, 
session_id INT, 
location VARCHAR, 
user_agent VARCHAR);""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users 
(user_id INT PRIMARY KEY, 
first_name VARCHAR, 
last_name VARCHAR, 
gender VARCHAR, 
level VARCHAR);""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs 
(song_id VARCHAR PRIMARY KEY, 
title VARCHAR, 
artist_id VARCHAR, 
year INT, 
duration NUMERIC);""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists 
(artist_id VARCHAR PRIMARY KEY, 
name VARCHAR, 
location VARCHAR, 
lattitude NUMERIC, 
longitude NUMERIC);""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time 
(start_time TIMESTAMP, 
hour INT, 
day INT, 
week INT, 
month INT, 
year INT, 
weekday VARCHAR);""")

staging_events_copy = ("""
copy staging_events from {}
iam_role {}
json{};
""").format(config['S3']['LOG_DATA'] , config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
copy staging_events from {}
iam_role {}
json'auto';
""").format(config['S3']['SONG_DATA'] , config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = "INSERT INTO songplays  \
(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) \
SELECT TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second' AS start_time,  \
e.userId AS user_id,  \
e.level,  \
s.song_id,  \
s.artist_id,  \
e.sessionId AS session_id,  \
s.artist_location AS location,  \
e.userAgent AS user_agent \
FROM staging_songs s \
RIGHT JOIN staging_events e \
ON s.title = e.song \
AND s.artist_name = e.artist \
AND s.duration = e.length \
WHERE page = 'NextSong';"

user_table_insert = "INSERT INTO users (user_id, first_name, last_name, gender, level) \
SELECT userId AS user_id,  \
firstName AS first_name,  \
lastName AS last_name,  \
gender,  \
level \
FROM staging_events;"
    
song_table_insert = "INSERT INTO songs (song_id, title, artist_id, year, duration) \
SELECT song_id,  \
title,  \
artist_id,  \
year,  \
duration \
FROM staging_songs;"
    
artist_table_insert = "INSERT INTO artists (artist_id, name, location, lattitude, longitude) \
SELECT artist_id,  \
artist_name AS name,  \
artist_location AS location,  \
artist_latitude AS lattitude,  \
artist_longitude AS longitude \
FROM staging_songs;"

time_table_insert = "INSERT INTO time (start_time, hour, day, week, month, year, weekday) \
SELECT DISTINCT start_time, \
EXTRACT (hour from start_time) AS hour, \
EXTRACT (day from start_time)AS day, \
EXTRACT (week from start_time) AS week, \
EXTRACT (month from start_time) AS month, \
EXTRACT (year from start_time) AS year, \
EXTRACT (dayofweek from start_time) AS weekday \
FROM songplays;"

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
