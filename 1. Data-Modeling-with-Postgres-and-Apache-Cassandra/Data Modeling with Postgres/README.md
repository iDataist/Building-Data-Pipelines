# Data Modeling with Postgres

## The Purpose of Creating the sparkifydb Database

Sparkify, a startup company with a music streaming app, would like to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. They want to create a Postgres database with tables designed to optimize queries on song play analysis.

## Schema for Song Play Analysis

### **Fact Table**
**1. songplays** - records in log data associated with song plays i.e. records with page NextSong
 - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### **Dimension Tables**
**2. users** - users in the app
 - *user_id, first_name, last_name, gender, level*

**3. songs** - songs in music database
 - *song_id, title, artist_id, year, duration*

**4. artists** - artists in music database
 - *artist_id, name, location, lattitude, longitude*

**5. time** - timestamps of records in songplays broken down into specific units
 - *start_time, hour, day, week, month, year, weekday*

## ETL Pipeline

1. [data](https://github.com/iDataist/Data-Engineering/tree/master/1.%20Data-Modeling-with-Postgres-and-Apache-Cassandra/Data%20Modeling%20with%20Postgres/data) contains song_data and log_data. 

2. [test.ipynb](https://github.com/iDataist/Data-Modeling-with-Postgres/blob/master/test.ipynb) displays the first few rows of each table to check the database.

3. [create_tables.py](https://github.com/iDataist/Data-Modeling-with-Postgres/blob/master/create_tables.py) drops and creates the tables, which needs to be run before the ETL scripts to reset the tables.

4. [etl.ipynb](https://github.com/iDataist/Data-Modeling-with-Postgres/blob/master/etl.ipynb) reads and processes a single file from song_data and log_data and loads the data into the tables.

5. [etl.py](https://github.com/iDataist/Data-Modeling-with-Postgres/blob/master/etl.py) reads and processes files from song_data and log_data and loads them into the tables.

6. [sql_queries.py](https://github.com/iDataist/Data-Modeling-with-Postgres/blob/master/sql_queries.py) contains all the sql queries, and is imported into the last three files above.

7. [README.md](https://github.com/iDataist/Data-Modeling-with-Postgres/blob/master/README.md) provides documentation on the project.
