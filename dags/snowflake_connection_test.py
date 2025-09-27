from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import pendulum

SNOWFLAKE_CONN_ID = 'snowflake_conn_id'

@dag(
    dag_id='snowflake_connection_test',
    start_date=pendulum.datetime(2025, 1, 1, tz='UTC'),
    schedule=None,
    catchup=False,
    tags=['test'],
)
def snowflake_connection_test_dag():

    @task
    def test_snowflake_connection():
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()
        result = cursor.execute('SELECT CURRENT_DATE;').fetchone()
        print('Snowflake connection successful! Current date is:', result[0])

    test_snowflake_connection()

test_dag = snowflake_connection_test_dag()