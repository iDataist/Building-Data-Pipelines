from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 7, 22),
    'depends_on_past': False, 
    'retries': 3, 
    'retry_delay': timedelta(minutes = 5), 
    'catchup': False, 
    'email_on_retry': False, 
    'email_on_failure': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag, 
    table='staging_events',
    redshift_conn_id='redshift',
    s3_bucket='udac-stg-bucket',
    s3_key='log_data/{0}-events.csv'.format('{{ds}}'), 
    delimiter = ',', 
    headers = '1', 
    quote_char = '"', 
    file_type = 'csv', 
    aws_credentials_id = 'aws_credentials'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag, 
    table='staging_songs',
    redshift_conn_id='redshift',
    s3_bucket='udac-stg-bucket,
    s3_key='song_data/', 
    delimiter = ',', 
    headers = '1', 
    quote_char = '"', 
    file_type = 'json', 
    aws_credentials_id = 'aws_credentials'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag, 
    table_name = 'songplays'
    redshift_conn_id='redshift',
    sql_statement = SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag, 
    table_name = 'users'
    redshift_conn_id='redshift',
    sql_statement = SqlQueries.user_table_insert, 
    append_data = False
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag, 
    table_name = 'songs'
    redshift_conn_id='redshift',
    sql_statement = SqlQueries.song_table_insert, 
    append_data = False
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag, 
    table_name = 'artists'
    redshift_conn_id='redshift',
    sql_statement = SqlQueries.artist_table_insert, 
    append_data = False
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag, 
    table_name = 'time'
    redshift_conn_id='redshift',
    sql_statement = SqlQueries.time_table_insert, 
    append_data = False
)

run_quality_checks_users = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
    redshift_conn_id='redshift',
    dq_checks = [
        {'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result': 0}, 
        {'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null", 'expected_result': 0}
    ]   
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift >> load_songplays_table
start_operator >> stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table >> run_quality_checks
load_songplays_table >> load_song_dimension_table >> run_quality_checks
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
run_quality_checks >> end_operator
