from datetime import datetime, timedelta
from plugins.operators import (StageToRedshiftOperator, LoadFactOperator, 
                               LoadDimensionOperator, DataQualityOperator)
from plugins.helpers import SqlQueries, ConfigureDataAccess
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.subdag import SubDagOperator

start_date = datetime.utcnow()
default_args = {
    'owner': 'udacity_learner_phuclh27',
    'start_date': start_date,
    'depend_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False
    
}

dag = DAG('sparkify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
        )

start_operator = EmptyOperator(task_id='Begin_execution', dag=dag)
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id=ConfigureDataAccess.REDSHIFT_CONN_ID,
    aws_credentials_id=ConfigureDataAccess.AWS_CREDENTIALS_ID,
    table='staging_events',
    s3_bucket=ConfigureDataAccess.S3_BUCKET,
    s3_key=ConfigureDataAccess.S3_LOG_KEY,
    region=ConfigureDataAccess.REGION,
    data_format=ConfigureDataAccess.DATA_FORMAT_EVENT
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id=ConfigureDataAccess.REDSHIFT_CONN_ID,
    aws_credentials_id=ConfigureDataAccess.AWS_CREDENTIALS_ID,
    table='staging_songs',
    s3_bucket=ConfigureDataAccess.S3_BUCKET,
    s3_key=ConfigureDataAccess.S3_SONG_KEY,
    region=ConfigureDataAccess.REGION,
    data_format=ConfigureDataAccess.DATA_FORMAT_SONG
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table_name='songplays',
    postgres_conn_id=ConfigureDataAccess.REDSHIFT_CONN_ID,
    sql_insert_stmt=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id="load_user_dim_table",
    dag=dag,
    table_name='users',
    postgres_conn_id=ConfigureDataAccess.REDSHIFT_CONN_ID,
    insert_sql_stmt=SqlQueries.user_table_insert,
    truncate=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id="load_song_dim_table",
    dag=dag,
    table_name='songs',
    postgres_conn_id=ConfigureDataAccess.REDSHIFT_CONN_ID,
    insert_sql_stmt=SqlQueries.song_table_insert,
    truncate=True
)


load_artist_dimension_table = LoadDimensionOperator(
    task_id="load_artist_dim_table",
    dag=dag,
    table_name='artists',
    postgres_conn_id=ConfigureDataAccess.REDSHIFT_CONN_ID,
    insert_sql_stmt=SqlQueries.artist_table_insert,
    truncate=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id="load_time_dim_table",
    dag=dag,
    table_name='time',
    postgres_conn_id=ConfigureDataAccess.REDSHIFT_CONN_ID,
    insert_sql_stmt=SqlQueries.time_table_insert,
    truncate=True
)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id=ConfigureDataAccess.REDSHIFT_CONN_ID,
    dq_checks_list=[
        { 'sql_testcase': 'SELECT COUNT(*) FROM public.users WHERE COALESCE(first_name, last_name, gender, level) IS NULL;', 'expected_result': 0 },
        { 'sql_testcase': 'SELECT COUNT(*) FROM public.songs WHERE COALESCE(title, artistid, year::text, duration::text) IS NULL;', 'expected_result': 0 },
        { 'sql_testcase': 'SELECT COUNT(*) FROM public.artists WHERE COALESCE(name, location, lattitude::text, longitude::text) IS NULL;', 'expected_result': 0 },
        { 'sql_testcase': 'SELECT COUNT(*) FROM public.time WHERE COALESCE(hour::text, day::text, week::text, month::text, year::text, weekday::text) is NULL;', 'expected_result': 0 }
    ]
)

end_operator = EmptyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_user_dimension_table, 
                         load_song_dimension_table, 
                         load_artist_dimension_table,
                         load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator