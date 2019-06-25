# Data Pipelines with Airflow

## The Purpose of Building a Data Pipeline with Airflow

Sparkify, a startup company with a music streaming app, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow. They would like to create
high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of CSV logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Datasets

There are two datasets that reside in S3. Here are the S3 links for each:

Song data: s3://udacity-dend/song_data

Log data: s3://udacity-dend/log_data

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

1. [etl.py](https://github.com/iDataist/Data-Engineering/blob/master/2.%20Data-Infrastructure-on-the-Cloud/etl.py) reads data from S3, processes that data using Spark, and writes them back to S3.

2. dl.cfg contains the AWS credentials.

3. [README.md](https://github.com/iDataist/Data-Engineering/blob/master/2.%20Data-Infrastructure-on-the-Cloud/README.md) provides documentation on the project.
