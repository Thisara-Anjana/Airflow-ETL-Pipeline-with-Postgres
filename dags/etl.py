from airflow import DAG
from airflow.decorators import task
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import json

##Define the DAG
with DAG(
    dag_id='nasa_apod_postgres',
    start_date=datetime(2026, 6, 18),
    schedule='@daily',
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
    ##https://api.nasa.gov/planetary/apod?api_key=7407pdm2ubQwUVTotl2AI22yulDTRATR1FHfZgla


    extract_apod = HttpOperator(
        task_id = 'extract_apod',
        http_conn_id = 'nasa_api', ##Connection ID defined in Airflow Connections
        endpoint = 'planetary/apod', ##Nasa API endpoint for APOD
        method = 'GET',
        data = {'api_key': "{{conn.nasa_api.extra_dejson.api_key}}"},##Use the API key from the connection
        response_filter = lambda response: response.json(), ##Convert the response to a JSON
    )


    ##step 3 : Transform the data (Pick the information that i need to save )

    @task
    def transform_apod_data(respone):
        apod_data ={
            'title': respone.get('title', ''),
            'explanation': respone.get('explanation', ''),
            'url': respone.get('url', ''),
            'date': respone.get('date', ''),
            'media_type': respone.get('media_type', '')
        }
        return apod_data

    ##step 4 : Load the data into Postgres SQL database

    @task
    def load_data_to_postgres(apod_data):
        ##initialize the Postgres hook
        postgres_hook = PostgresHook(postgres_conn_id="my_postgres_connection")

        ##Define the SQL query
        insert_query = """
        INSERT INTO apod (title, explanation, url, date, media_type)
        VALUES (%s, %s, %s, %s, %s);
        """
        ##Execute the query
        postgres_hook.run(insert_query, parameters=(apod_data['title'], 
                                                    apod_data['explanation'], apod_data['url'], 
                                                    apod_data['date'], apod_data['media_type']))

    ##step 5 : Verify the data DBViewer 

    

    ##Step 6 : Define the task dependencies

    create_table() >> extract_apod ##Ensure the table is created before extracting data
    api_response = extract_apod.output ##Get the API response from the extraction task
    transformed_data = transform_apod_data(api_response) ##Transform the data
    load_data_to_postgres(transformed_data) ##Load the transformed data into Postgres