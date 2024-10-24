from airflow.decorators import task
from airflow import DAG
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import logging


def return_snowflake_conn():
    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    # Get the Snowflake connection
    conn = hook.get_conn()
    return conn.cursor()


@task
def run_ctas(table, select_sql, primary_key=None):
    logging.info(f"Creating or replacing table: {table}")
    logging.info(f"Executing select SQL: {select_sql}")

    cur = return_snowflake_conn()

    try:
        # Start transaction
        cur.execute("BEGIN;")
        sql = f"CREATE OR REPLACE TABLE {table} AS {select_sql}"
        logging.info(f"Running SQL: {sql}")
        cur.execute(sql)

        # Primary key uniqueness check
        if primary_key is not None:
            uniqueness_sql = f"""
            SELECT {primary_key}, COUNT(1) AS cnt 
            FROM {table} 
            GROUP BY {primary_key} 
            ORDER BY cnt DESC 
            LIMIT 1
            """
            logging.info(f"Checking primary key uniqueness: {uniqueness_sql}")
            cur.execute(uniqueness_sql)
            result = cur.fetchone()

            if int(result[1]) > 1:
                raise Exception(f"Primary key uniqueness failed: {result}")

        # Commit transaction
        cur.execute("COMMIT;")
        logging.info(f"Table {table} created successfully.")

    except Exception as e:
        cur.execute("ROLLBACK;")
        logging.error('Failed to execute SQL. ROLLBACK completed!')
        raise


# Define the DAG
with DAG(
    dag_id='BuildELT_CTAS',
    start_date=datetime(2024, 10, 2),
    schedule_interval='45 2 * * *',  # Run daily at 2:45 AM
    catchup=False,
    tags=['ELT']
) as dag:

    table = "dev.analytics.session_summary"
    select_sql = """
    SELECT u.*, s.ts
    FROM dev.raw_data.user_session_channel u
    JOIN dev.raw_data.session_timestamp s 
    ON u.sessionId = s.sessionId
    """

    # Run the task to create the session_summary table
    run_ctas(table, select_sql, primary_key='sessionId')
