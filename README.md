# Airflow Snowflake ETL/ELT Pipeline

This repository demonstrates how to set up and run an ETL/ELT pipeline using **Apache Airflow** and **Snowflake**. The pipeline includes creating and populating two Snowflake tables, followed by an Airflow DAG that creates a `session_summary` table by joining the two tables.

## Table of Contents
1. [Prerequisites](#prerequisites)
2. [Step 1: Setup Snowflake Tables](#step-1-setup-snowflake-tables)
3. [Step 2: Populate Snowflake Tables](#step-2-populate-snowflake-tables)
4. [Step 3: Build and Run Airflow ELT DAG](#step-3-build-and-run-airflow-elt-dag)
5. [Troubleshooting](#troubleshooting)

---

## Prerequisites
- **Snowflake Account**: Ensure you have access to a Snowflake account and permissions to create schemas, tables, and run queries.
- **Apache Airflow**: Installed and set up for running DAGs.
- **S3 Bucket**: Read access to the S3 bucket `s3://s3-geospatial/readonly/` containing CSV files.

### Snowflake Connection Setup in Airflow
Ensure you have configured the Snowflake connection (`snowflake_conn`) in Airflow:
1. Navigate to Airflow UI > Admin > Connections.
2. Create a new connection with the following details:
   - **Conn ID**: `snowflake_conn`
   - **Conn Type**: Snowflake
   - **Extra**: JSON containing account, role, database, and schema information.

---

## Step 1: Setup Snowflake Tables

You need to create two tables in your Snowflake account:

### SQL for Table Creation:

```sql
CREATE TABLE IF NOT EXISTS dev.raw_data.user_session_channel (
    userId int not NULL,
    sessionId varchar(32) primary key,
    channel varchar(32) default 'direct'  
);

CREATE TABLE IF NOT EXISTS dev.raw_data.session_timestamp (
    sessionId varchar(32) primary key,
    ts timestamp  
);
