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

## Data Pipeline

### 1. The [DAG](https://github.com/iDataist/Music-Streaming-App-Data-Engineering/tree/master/4.%20Data-Pipelines-with-Airflow/airflow/dags)
- The DAG does not have dependencies on past runs
- On failure, the task are retried 3 times
- Retries happen every 5 minutes
- Catchup is turned off
- Do not email on retry
- The graph below shows task dependencies
![DAG](example-dag.png)

### 2. The SQL [helpers](https://github.com/iDataist/Music-Streaming-App-Data-Engineering/tree/master/4.%20Data-Pipelines-with-Airflow/airflow/plugins/helpers) to run data transformations

### 3. The [operators](https://github.com/iDataist/Music-Streaming-App-Data-Engineering/tree/master/4.%20Data-Pipelines-with-Airflow/airflow/plugins/operators) to perform tasks such as staging the data, filling the data warehouse, and running checks on the data

- **Stage Operator:**
The stage operator loads any JSON and CSV formatted files from S3 to Amazon Redshift. The operator creates and runs a SQL COPY statement based on the parameters provided. The operator's parameters specify where in S3 the file is loaded and what is the target table. Parameters are used to distinguish between JSON and CSV file. The stage operator contains a templated field that allows it to load timestamped files from S3 based on the execution time and run backfills.

- **Fact and Dimension Operators:**
The dimension and fact operators utilize the SQL helper class to run data transformations. The operators take as input a SQL statement and target database on which to run the query against. Dimension loads are often done with the truncate-insert pattern where the target table is emptied before the load. Thus, there is a parameter that allows switching between insert modes when loading dimensions. Fact tables are usually so massive that they only allow append type functionality.

- **Data Quality Operator:**
The data quality operator is used to run checks on the data itself. The operator's main functionality is to receive one or more SQL based test cases along with the expected results and execute the tests. For each of the test, the test result and expected result are checked. If there is no match, the operator raises an exception and the task retries and fails eventually.

### 4. [README.md](https://github.com/iDataist/Music-Streaming-App-Data-Engineering/blob/master/4.%20Data-Pipelines-with-Airflow/README.md) to provide documentation on the project
