from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common.final_project_sql_statements import SqlQueries
from airflow.operators.postgres_operator import PostgresOperator

default_args = {
    'owner': 'Reginald',
    'depends_on_past': False, # The DAG does not have dependencies on past runs
    'start_date': pendulum.now(), 
    'retries': 3, # On failure, the task are retried 3 times
    'retry_delay': timedelta(minutes=5), # Retries happen every 5 minutes
    'catchup': False, # Catchup is turned off
    'email_on_retry': False # Do not email on retry
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')
    end_operator = DummyOperator(task_id='End_execution')

    create_staging_events_table = PostgresOperator(
        task_id="create_staging_events_table",
        postgres_conn_id="redshift",
        sql=SqlQueries.staging_events_table_create
    ) 
    create_staging_songs_table = PostgresOperator(
        task_id="create_staging_songs_table",
        postgres_conn_id="redshift",
        sql=SqlQueries.staging_songs_table_create
    )
    create_songplay_table = PostgresOperator(
        task_id="create_songplay_table",
        postgres_conn_id="redshift",
        sql=SqlQueries.songplay_table_create
    )
    create_user_table = PostgresOperator(
        task_id="create_user_table",
        postgres_conn_id="redshift",
        sql=SqlQueries.user_table_create
    )
    create_song_table = PostgresOperator(
        task_id="create_song_table",
        postgres_conn_id="redshift",
        sql=SqlQueries.song_table_create
    )
    create_artist_table = PostgresOperator(
        task_id="create_artist_table",
        postgres_conn_id="redshift",
        sql=SqlQueries.artist_table_create
    )
    create_time_table = PostgresOperator(
        task_id="create_time_table",
        postgres_conn_id="redshift",
        sql=SqlQueries.time_table_create
    ) 

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table="log_data",
        redshift_conn_id="redshift",
        ARN="arn:aws:iam::101621241983:role/dwhRole", 
        aws_credentials_id="aws_credentials",
        s3_bucket="udacity-dend",
        s3_key="log_data",
        s3_location="s3://udacity-dend/log_data",
        json_format="s3://udacity-dend/log_json_path.json"
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table="song_data",
        redshift_conn_id="redshift",
        ARN="arn:aws:iam::101621241983:role/dwhRole", 
        aws_credentials_id="aws_credentials",
        s3_bucket="udacity-dend",
        s3_key="song_data/A/A",
        s3_location="s3://udacity-dend/song_data/A/A",
        json_format = "auto"
    )


    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id="redshift",
        sql=SqlQueries.songplay_table_insert
    )




    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id="redshift",
        sql_create=SqlQueries.user_table_insert,
        sql_insert=SqlQueries.user_table_insert
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id="redshift",
        sql_create=SqlQueries.song_table_insert,
        sql_insert=SqlQueries.song_table_insert
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id="redshift",
        sql_create=SqlQueries.artist_table_insert,
        sql_insert=SqlQueries.artist_table_insert
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id="redshift",
        sql_create=SqlQueries.time_table_insert,
        sql_insert=SqlQueries.time_table_insert
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id="redshift",
        params={
            'tables': ["log_data", "song_data", # "factSongPlays", 
                        "dimArtists", "dimTime", "dimUsers", "dimSongs"]
            
        }

    )

    start_operator >> create_staging_events_table
    start_operator >> create_staging_songs_table

    create_staging_events_table >> stage_events_to_redshift

    create_staging_songs_table >> stage_songs_to_redshift

    stage_events_to_redshift >> create_songplay_table
    stage_songs_to_redshift >> create_songplay_table

    create_songplay_table >> load_songplays_table

    load_songplays_table >> create_song_table
    load_songplays_table >> create_user_table
    load_songplays_table >> create_artist_table
    load_songplays_table >> create_time_table


    create_song_table >> load_song_dimension_table
    create_user_table >> load_user_dimension_table
    create_artist_table >> load_artist_dimension_table
    create_time_table >> load_time_dimension_table




    load_song_dimension_table >> run_quality_checks
    load_user_dimension_table >> run_quality_checks
    load_artist_dimension_table >> run_quality_checks
    load_time_dimension_table >> run_quality_checks

    run_quality_checks >> end_operator




final_project_dag = final_project()