import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
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
length NUMERIC,
level VARCHAR, 
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

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
