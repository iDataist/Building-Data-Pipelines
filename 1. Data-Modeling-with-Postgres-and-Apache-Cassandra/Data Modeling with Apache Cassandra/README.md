# Data Modeling with Apache Cassandra

## The Purpose of Creating the Apache Cassandra Database
Sparkify, a startup company with a music streaming app, would like to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app. They want to create an Apache Cassandra database which can create queries on song play data to answer the questions.

## ETL Pipeline

1. [event_data](https://github.com/iDataist/Data-Engineering/tree/master/1.%20Data-Modeling-with-Postgres-and-Apache-Cassandra/Data%20Modeling%20with%20Apache%20Cassandra/event_data) contains the CSV files of user activity on the app

2. [etl.ipynb](https://github.com/iDataist/Data-Engineering/blob/master/1.%20Data-Modeling-with-Postgres-and-Apache-Cassandra/Data%20Modeling%20with%20Apache%20Cassandra/etl.ipynb)

  - Displays the first few rows of the CSV file in event_data to check the data

  - Reads and processes the CSV files from event_data, and loads them into a denormalized dataset

  - Writes the dataset into event_datafile_new.csv

  - Creates and drops the tables in Apache Cassandra

  - Loads the data from event_datafile_new.csv into tables and run queries

3. [event_datafile_new.csv](https://github.com/iDataist/Data-Engineering/blob/master/1.%20Data-Modeling-with-Postgres-and-Apache-Cassandra/Data%20Modeling%20with%20Apache%20Cassandra/event_datafile_new.csv) contains the denormalized dataset processed from all the CSV files in the event_data

4. [README.md](https://github.com/iDataist/Data-Engineering/blob/master/1.%20Data-Modeling-with-Postgres-and-Apache-Cassandra/Data%20Modeling%20with%20Apache%20Cassandra/README.md) provides documentation on the project
