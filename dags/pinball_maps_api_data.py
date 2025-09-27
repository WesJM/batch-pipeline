# dags/dynamic_api_dag.py

from __future__ import annotations

import pendulum
import requests
import logging
import json
from datetime import timedelta

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.decorators import dag, task
from config.api_config import endpoints

DATABASE = 'PINBALL_MAPS_API_DB'
SCHEMA = 'API_FETCH_SCHEMA'
SNOWFLAKE_CONN_ID = 'snowflake_conn_id'

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def create_dynamic_api_dag(endpoint):
    """
    Function to generate a dynamic DAG for a given API endpoint.
    """
    dag_id = f"dynamic_api_{endpoint['name']}"

    @dag(
        dag_id=dag_id,
        default_args=default_args,
        start_date=pendulum.datetime(2025, 1, 1, tz='UTC'),
        schedule='@weekly',
        catchup=False,
        tags=['rest_api', 'dynamic', 'snowflake'],
    )
    def api_etl_dag():
        """
        Fetches JSON from REST API with error handling and passes results to downstream task.
        """
        @task
        def fetch_data(api_url: str):
            """Fetch data from the specified API endpoint."""
            logging.info(f'Fetching data from: {api_url}') 

            try:
                response = requests.get(api_url, timeout=30)
                response.raise_for_status()  # Raises HTTPError for bad responses
                logging.info(f'Data fetched successfully: {len(response.text)} bytes')
                data = response.json()

                # Data is pushed to xCom automatically
                return data
            
            except requests.exceptions.RequestException as e:
                logging.error(f'API request failed: {e}')
                raise
        @task
        def load_raw(data: dict):
            hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
            
            staging_table = f"{DATABASE}.{SCHEMA}.{endpoint['stage_table']}"
            
            sql = f'''
                INSERT INTO {staging_table} (payload)
                SELECT PARSE_JSON(%(payload)s)
            '''
            hook.run(
                sql,
                parameters={
                    'payload': json.dumps(data)  # full response dict → JSON string
                }
            )

        # Task dependencies
        data = fetch_data(api_url=endpoint['url'])
        load_raw(data)

    return api_etl_dag()

# Use a loop to call the factory function for each endpoint
for endpoint in endpoints:
    globals()[f"dag_{endpoint['name']}"] = create_dynamic_api_dag(endpoint)