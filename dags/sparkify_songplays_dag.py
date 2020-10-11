from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'iamfeniak',
    'depends_on_past': False,
    'start_date': datetime(2020, 10, 7),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('sparkify_songplays_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
          )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    connection_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    json_format="s3://udacity-dend/log_json_path.json",
    provide_context=True,
    dag=dag
)
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    connection_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    fact_table='songplays',
    sql_statement=SqlQueries.songplay_table_insert,
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dimension_table='users',
    sql_statement=SqlQueries.user_table_insert,
    mode='truncate-insert',
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dimension_table='songs',
    sql_statement=SqlQueries.song_table_insert,
    mode='truncate-insert',
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dimension_table='artists',
    sql_statement=SqlQueries.artist_table_insert,
    mode='truncate-insert',
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dimension_table='time',
    sql_statement=SqlQueries.time_table_insert,
    mode='truncate-insert',
    dag=dag
)

quality_checks = DataQualityOperator(
    task_id="Run_data_quality_checks",
    connection_id="redshift",
    test_case_statements=[SqlQueries.rows_count_check],
    test_case_expected_outcomes=[1],
    data_sources=["staging_events", "staging_songs", "songplays", "users", "songs", "artists", "time"],
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# DAG run sequence
start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table,
                         load_time_dimension_table] >> quality_checks >> end_operator
