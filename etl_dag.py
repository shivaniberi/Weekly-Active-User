from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago

# Define default args
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'depends_on_past': False,
}

# SQL Queries for creating tables and loading data
create_user_session_channel_table = """
CREATE TABLE IF NOT EXISTS dev.raw_data.user_session_channel (
    userId int not NULL,
    sessionId varchar(32) primary key,
    channel varchar(32) default 'direct'
);
"""

create_session_timestamp_table = """
CREATE TABLE IF NOT EXISTS dev.raw_data.session_timestamp (
    sessionId varchar(32) primary key,
    ts timestamp
);
"""

create_stage = """
CREATE OR REPLACE STAGE dev.raw_data.blob_stage
url = 's3://s3-geospatial/readonly/'
file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"');
"""

copy_user_session_channel = """
COPY INTO dev.raw_data.user_session_channel
FROM @dev.raw_data.blob_stage/user_session_channel.csv;
"""

copy_session_timestamp = """
COPY INTO dev.raw_data.session_timestamp
FROM @dev.raw_data.blob_stage/session_timestamp.csv;
"""

# Define the DAG
with DAG(
    'snowflake_import_dag',
    default_args=default_args,
    description='ETL DAG to import data into Snowflake',
    schedule_interval=None,  # Manual trigger
    catchup=False,
    tags=['snowflake', 'ETL'],
) as dag:

    # Create user_session_channel table
    create_user_session_channel = SnowflakeOperator(
        task_id='create_user_session_channel_table',
        sql=create_user_session_channel_table,
        snowflake_conn_id='snowflake_conn',
    )

    # Create session_timestamp table
    create_session_timestamp = SnowflakeOperator(
        task_id='create_session_timestamp_table',
        sql=create_session_timestamp_table,
        snowflake_conn_id='snowflake_conn',
    )

    # Create Snowflake stage
    create_stage = SnowflakeOperator(
        task_id='create_stage',
        sql=create_stage,
        snowflake_conn_id='snowflake_conn',
    )

    # Copy data into user_session_channel
    load_user_session_channel = SnowflakeOperator(
        task_id='copy_user_session_channel',
        sql=copy_user_session_channel,
        snowflake_conn_id='snowflake_conn',
    )

    # Copy data into session_timestamp
    load_session_timestamp = SnowflakeOperator(
        task_id='copy_session_timestamp',
        sql=copy_session_timestamp,
        snowflake_conn_id='snowflake_conn',
    )

    # Define task dependencies
    create_user_session_channel >> create_session_timestamp >> create_stage
    create_stage >> [load_user_session_channel, load_session_timestamp]