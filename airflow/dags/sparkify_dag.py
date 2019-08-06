from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

from airflow.models import Variable

# Using Airflow's Variable to for accessing private data
dag_config = Variable.get("Sparkify_configs", deserialize_json=True)
s3_bucket = dag_config["s3_bucket"]
s3_logdata_key = dag_config["s3_logdata_key"]
s3_songdata_key = dag_config["s3_songdata_key"]

default_args = {
    'owner': 'Adrien',
    'start_date': datetime(2019, 1, 12),
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load from s3 and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_events',
    dag = dag,
    table = 'staging_events',
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials_id',
    s3_key = s3_key,
    s3_bucket = s3_bucket
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table = 'staging_songs',
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials_id',
    s3_key = s3_key,
    s3_bucket = s3_bucket
)

load_songplays_table = LoadFactOperator(
    task_id = 'Load_songplays_fact_table',
    dag = dag,
    table = 'songplays',
    fields= 'playid, start_time, userid, level, songid, artistid, sessionid, location, user_agent',
    redshift_conn_id = 'redshift',
    load_facts_sql = SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table='users',
    fields='userid, first_name, last_name, gender, level',
    redshift_conn_id = 'redshift',
    load_facts_sql = SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table='songs',
    fields='songid, title, artistid, year, duration ',
    redshift_conn_id = 'redshift',
    load_facts_sql = SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table='artists',
    fields='artistid, name, location, lattitude, longitude',
    redshift_conn_id = 'redshift',
    load_facts_sql = SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table='times',
    fields='start_time, hour, day, week, month, year, week_day',
    redshift_conn_id = 'redshift',
    load_facts_sql = SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    tables=['songplays', 'songs', 'artists', 'users', 'times'],
    redshift_conn_id='redshift',
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
