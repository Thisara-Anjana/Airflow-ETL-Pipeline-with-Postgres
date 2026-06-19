from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import json

##Define the DAG
with DAG(
    dag_id='nasa_apod_postgres',
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    ##step 1 : Create the tables if not exists

    @task
    def create_table():
        ##initialize the Postgres hook
        postgres_hook = PostgresHook(postgres_conn_id="my_postgres_connection")

        ##SQL Query to create the table 
        create_table_query = """
        CREATE TABLE IF NOT EXISTS apod (
        id SERIAL PRIMARY KEY,
        title VARCHAR(255),
        explanation TEXT,
        url TEXT,
        date DATE,
        media_type VARCHAR(50)
        );
        """
        ## Execute the query
        postgres_hook.run(create_table_query)


    ##step 2 : Extract the data from the API data (APOD) Astronomy Picture of the Day


    ##step 3 : Transform the data (Pick the information that i need to save )
    
    ##step 4 : Load the data into Postgres SQL database

    ##step 5 : Verify the data DBViewer 

    ##Step 6 : Define the task dependencies

    