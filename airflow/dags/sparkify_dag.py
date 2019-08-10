from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator, CreateTableRedshiftOperator)

from helpers import SqlQueries
from helpers import CreateTableSqlQueries
from airflow.models import Variable

# Using Airflow's Variable to for accessing source data config
#   Including s3 bucket and Keys

dag_config = Variable.get("Sparkify_configs", deserialize_json=True)
s3_bucket = dag_config["s3_bucket"]
s3_logdata_key = dag_config["s3_logdata_key"]
s3_songdata_key = dag_config["s3_songdata_key"]
log_jsonpath_file = dag_config["LOG_JSONPATH"]
quotations = '\"'
s3_full_jsonpath = f"'s3://{s3_bucket.strip(quotations)}/{log_jsonpath_file.strip(quotations)}'"

default_args = {
    'owner': 'Adrien',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 12),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('sparkify_dag',
          default_args=default_args,
          description='Load from s3 and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

## Creating tables first in Redshift ##
#######################################
songplay_table_create = CreateTableRedshiftOperator(
    task_id = 'create_songplays_table',
    dag = dag,
    table = 'songplays',
    redshift_conn_id = 'redshift',
    create_table_sql = CreateTableSqlQueries.songplay_table_create
)

artist_table_create = CreateTableRedshiftOperator(
    task_id = 'create_artist_table',
    dag = dag,
    table = 'artists',
    redshift_conn_id = 'redshift',
    create_table_sql = CreateTableSqlQueries.artist_table_create
)

song_table_create = CreateTableRedshiftOperator(
    task_id = 'create_songs_table',
    dag = dag,
    table = 'songs',
    redshift_conn_id = 'redshift',
    create_table_sql = CreateTableSqlQueries.song_table_create
)

user_table_create = CreateTableRedshiftOperator(
    task_id = 'create_users_table',
    dag = dag,
    table = 'users',
    redshift_conn_id = 'redshift',
    create_table_sql = CreateTableSqlQueries.user_table_create
)

time_table_create = CreateTableRedshiftOperator(
    task_id = 'create_times_table',
    dag = dag,
    table = 'times',
    redshift_conn_id = 'redshift',
    create_table_sql = CreateTableSqlQueries.time_table_create
)

staging_events_table_create = CreateTableRedshiftOperator(
    task_id = 'create_staging_events_table',
    dag = dag,
    table = 'staging_events',
    redshift_conn_id = 'redshift',
    create_table_sql = CreateTableSqlQueries.staging_events_table_create
)

staging_songs_table_create = CreateTableRedshiftOperator(
    task_id = 'create_staging_songs_table',
    dag = dag,
    table = 'staging_songs',
    redshift_conn_id = 'redshift',
    create_table_sql = CreateTableSqlQueries.staging_songs_table_create
)


## LOADING staging tables in redshit ##
#######################################
stage_events_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_events',
    dag = dag,
    table = 'staging_events',
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials_id',
    s3_key = s3_logdata_key,
    s3_bucket = s3_bucket,
    JSON_formatting = s3_full_jsonpath,
    append_data = False
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table = 'staging_songs',
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials_id',
    s3_key = s3_songdata_key,
    s3_bucket = s3_bucket,
    JSON_formatting="'auto' truncatecolumns",
    append_data = False
)

## LOADING facts tables on song plays in redshit ##
###################################################
load_songplays_table = LoadFactOperator(
    task_id = 'Load_songplays_fact_table',
    dag = dag,
    table = 'songplays',
    fields= 'playid, start_time, userid, level, songid, artistid, sessionid, location, user_agent',
    redshift_conn_id = 'redshift',
    load_facts_sql = SqlQueries.songplay_table_insert,
    append_data = False
)


## LOADING dimensions tables on song plays in redshit ##
########################################################
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table='users',
    fields='userid, first_name, last_name, gender, level',
    redshift_conn_id = 'redshift',
    load_dimension = SqlQueries.user_table_insert,
    append_data = False
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table='songs',
    fields='songid, title, artistid, year, duration ',
    redshift_conn_id = 'redshift',
    load_dimension = SqlQueries.song_table_insert,
    append_data = False
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table='artists',
    fields='artistid, name, location, lattitude, longitude',
    redshift_conn_id = 'redshift',
    load_dimension = SqlQueries.artist_table_insert,
    append_data = False
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table='times',
    fields='start_time, hour, day, week, month, year, week_day',
    redshift_conn_id = 'redshift',
    load_dimension = SqlQueries.time_table_insert,
    append_data = False
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    tables=['songplays', 'songs', 'artists', 'users', 'times'],
    redshift_conn_id='redshift',
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)





## Task execution ordering ##
#############################

start_operator >> songplay_table_create
start_operator >> artist_table_create
start_operator >> song_table_create
start_operator >> user_table_create
start_operator >> time_table_create
start_operator >> staging_events_table_create
start_operator >> staging_songs_table_create


songplay_table_create >> stage_events_to_redshift
artist_table_create >> stage_events_to_redshift
song_table_create >> stage_events_to_redshift
user_table_create >> stage_events_to_redshift
time_table_create >> stage_events_to_redshift
staging_events_table_create >> stage_events_to_redshift
staging_songs_table_create >> stage_events_to_redshift

songplay_table_create >> stage_songs_to_redshift
artist_table_create >> stage_songs_to_redshift
song_table_create >> stage_songs_to_redshift
user_table_create >> stage_songs_to_redshift
time_table_create >> stage_songs_to_redshift
staging_events_table_create >> stage_songs_to_redshift
staging_songs_table_create >> stage_songs_to_redshift

# start_operator >> stage_events_to_redshift
# start_operator >> stage_songs_to_redshift

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

run_quality_checks >> end_operator
