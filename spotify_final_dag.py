import datetime as dt
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from sqlalchemy import create_engine

from airflow.utils.dates import days_ago

from spotify_etl import spotify_etl

default_args={
    'owner':'airlfow',
    'depends_on_past':False,
    'start_date':dt.datetime(2023,9,17),
    'email':['tavneetmanchanda@gmail.com'],
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':1,
    'retry_delay':dt.timedelta(minutes=1)

}
dag=DAG(
    'spotify_final_dag',
    default_args=default_args,
    description="Spotify ETL dag processs 2 min",
    schedule_interval=dt.timedelta(minutes=50),
)

def ETL():
    print("Started")
    df=spotify_etl()
    conn=BaseHook.get_connection('post1')
    
    engine=create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    df.to_sql('my_played_tracks',engine,if_exists='replace')

# with dag:
#     create_table=PostgresOperator(
#         task_id='create_table',
#         postgres_conn_id='post1',  ## this is the connection id which was made using airflow
#         sql= """

#            CREATE TABLE IF NOT EXISTS my_played_tracks(
#            song_name VARCHAR(200),
#            artist_name VARCHAR(200)
#         )
        
#         """
#     )



with dag:    
    create_table= PostgresOperator(
        task_id='create_table',
        postgres_conn_id='post1',
        sql="""
            CREATE TABLE IF NOT EXISTS my_played_tracks(
            song_name VARCHAR(200),
            artist_name VARCHAR(200)
          
        )
        """
    )

    run_etl=PythonOperator(
        task_id='spotify_etl_final',
        python_callable=ETL,
        dag=dag,
    )

    create_table >> run_etl