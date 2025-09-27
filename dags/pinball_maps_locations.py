"""
Airflow DAG: Ingests regional pinball machine location data from the Pinball Maps API
into Snowflake for downstream analytics. Scheduled to run weekly with retries and logging enabled.
"""

from __future__ import annotations

import pendulum
import requests
import logging
import json
from datetime import timedelta

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.decorators import dag, task


DATABASE = 'PINBALL_MAPS_API_DB'
SCHEMA = 'API_FETCH_SCHEMA'
SNOWFLAKE_CONN_ID = 'snowflake_conn_id'
STAGING_TABLE =  f'{DATABASE}.{SCHEMA}.STG_LOCATIONS_RAW'

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='pinball_maps_locations',
    start_date=pendulum.datetime(2025, 1, 1, tz='UTC'),
    schedule='@weekly',
    catchup=False,
    tags=['rest_api', 'locations', 'snowflake'],
)
def locations_etl_dag():
    """
    Fetches location data per region from the Pinball Maps API
    and loads it into a Snowflake staging table.
    """

    @task
    def fetch_region_ids() -> list[dict]:
        """
        Pull distinct region IDs and names from Snowflake.
        Returns a list of dicts: [{ "region_id": <id>, "region_name": <name> }, ...]
        """ 
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        sql = f'''
            SELECT DISTINCT region_id, region_name 
            FROM {DATABASE}.{SCHEMA}.DIM_REGIONS 
            ORDER BY region_name
        '''
        results = hook.get_pandas_df(sql)

        regions = [{'region_id': row[0], 'region_name': row[1]} for row in results.values]
        logging.info(f'Fetched {len(regions)} regions from Snowflake')
        
        return regions
        
    @task
    def fetch_locations(regions: list[dict]) -> list[dict]:
        """
        For each region, call the Pinball Maps API and collect location payloads.
        Returns a list of dicts: { "region_id": <id>, "region_name": <name>, "payload": <json> }
        """
        all_payloads = []
        url = 'https://pinballmap.com/api/v1/locations.json'

        for region in regions:
            region_id = region['region_id']
            region_name = region['region_name']

            try:
                logging.info(f'Fetching locations for region: {region_name} ({region_id})')
                resp = requests.get(url, params={'region': region_name}, timeout=30)
                resp.raise_for_status()
                payload = resp.json()

                all_payloads.append(
                    {'region_id': region_id, 'region_name': region_name, 'payload': payload}
                )

                locations = payload.get('locations', [])
                logging.info(f'Fetched {len(locations)} locations for region {region_name}')

            except requests.exceptions.RequestException as e:
                logging.error(f'API request failed for region {region_name} ({region_id}): {e}')
                raise

        return all_payloads

    @task
    def load_raw(all_payloads: list[dict]):
        """
        Insert each region's location payload into Snowflake staging table.
        NOTE: future improvement is collect entries into a single VALUES clause and then insert once per DAG run
        """
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        insert_count = 0

        for entry in all_payloads:
            sql = f'''
                INSERT INTO {STAGING_TABLE} (region_id, region_name, payload, ingested_at)
                SELECT %(region_id)s, %(region_name)s, PARSE_JSON(%(payload)s), CURRENT_TIMESTAMP
            '''

            region_id = entry['region_id']
            region_name = entry['region_name']

            try:

                hook.run(
                    sql,
                    parameters={
                        'region_id': region_id,
                        'region_name': region_name,
                        'payload': json.dumps(entry['payload']),
                    },
                )
                logging.info(f'Inserted locations for region {region_name} ({region_id}) into Snowflake')
                insert_count += 1
            
            except Exception as e:
                logging.error(f'Failed to insert locations for region {region_name} ({region_id}): {e}')
                raise
        
        logging.info(f'Inserted {insert_count} regions into Snowflake')

    # DAG dependencies
    region_ids = fetch_region_ids()
    payloads = fetch_locations(region_ids)
    load_raw(payloads)


locations_etl_dag()