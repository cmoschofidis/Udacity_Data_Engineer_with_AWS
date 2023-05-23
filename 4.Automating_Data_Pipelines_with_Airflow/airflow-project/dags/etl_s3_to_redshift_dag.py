from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from plugins.helpers import sql_statements


default_args = {
    "owner": "marcelo",
    "start_date": pendulum.now(),
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
    "email_on_retry": False
}

@dag(
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval='0 * * * *'
)
def final_project():

    start_operator = DummyOperator(task_id="Begin_execution")

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id="Stage_events",
        aws_credentials_id="aws_credentials",
        redshift_conn_id="redshift",
        table="staging_events",
        s3_path="s3://udacity-airflow-project-marcelo/log-data/2018/11/",
        log_json_file="s3://udacity-dend/log_json_path.json",
        provide_context=True,
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id="Stage_songs",
        aws_credentials_id="aws_credentials",
        redshift_conn_id="redshift",
        table="staging_songs",
        s3_path="s3://udacity-airflow-project-marcelo/song-data/A/A/A/",
        provide_context=True,
    )

    load_songplays_table = LoadFactOperator(
        task_id="Load_songplays_fact_table",
        redshift_conn_id="redshift",
        table="songplays",
        sql_query=final_project_sql_statements.songplay_table_insert,
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id="Load_user_dim_table",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        sql_query=final_project_sql_statements.user_table_insert,
        table="users",
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id="Load_song_dim_table",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        sql_query=final_project_sql_statements.song_table_insert,
        table="songs",
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id="Load_artist_dim_table",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        sql_query=final_project_sql_statements.artist_table_insert,
        table="artists",
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id="Load_time_dim_table",
        redshift_conn_id="redshift",
        sql_query=final_project_sql_statements.time_table_insert,
        table="time",
    )

    run_quality_checks = DataQualityOperator(
        task_id="Run_data_quality_checks",
        redshift_conn_id="redshift",
        tables=["artists", "songplays", "songs", "time", "users"],
    )

    end_operator = DummyOperator(task_id="End_execution")

    start_operator >> stage_events_to_redshift
    start_operator >> stage_songs_to_redshift

    stage_events_to_redshift >> load_songplays_table
    stage_songs_to_redshift >> load_songplays_table

    (
    load_songplays_table
    >> [
        load_user_dimension_table,
        load_song_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table,
    ]
    >> run_quality_checks   
    >> end_operator
    )


final_project_dag = final_project()