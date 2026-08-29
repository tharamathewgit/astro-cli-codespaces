from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.databricks.operators.databricks import (
    DatabricksRunNowOperator,
)
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="taxi_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 8, 16),
    schedule=None,
    catchup=False,
    tags=["taxi", "databricks", "snowflake"],
) as dag:

    # 1. Run the Databricks job
    run_databricks = DatabricksRunNowOperator(
        task_id="run_databricks",
        databricks_conn_id="databricks_default",
        job_id=778776907351131,
    )

    # 2. Load dimension tables into Snowflake

    load_dim_payment = SQLExecuteQueryOperator(
        task_id="load_dim_payment",
        conn_id="snowflake_conn",
        sql="""
            COPY INTO TAXI_DB.STAR.DIM_PAYMENT
            FROM @TAXI_DB.STAR.TAXI_STAGE/dim_payment.csv.gz
            FILE_FORMAT = (
                FORMAT_NAME = TAXI_DB.STAR.TAXI_CSV_FORMAT
            );
        """,
    )

    load_dim_rate = SQLExecuteQueryOperator(
        task_id="load_dim_rate",
        conn_id="snowflake_conn",
        sql="""
            COPY INTO TAXI_DB.STAR.DIM_RATE
            FROM @TAXI_DB.STAR.TAXI_STAGE/dim_rate.csv.gz
            FILE_FORMAT = (
                FORMAT_NAME = TAXI_DB.STAR.TAXI_CSV_FORMAT
            );
        """,
    )

    load_dim_date = SQLExecuteQueryOperator(
        task_id="load_dim_date",
        conn_id="snowflake_conn",
        sql="""
            COPY INTO TAXI_DB.STAR.DIM_DATE
            FROM @TAXI_DB.STAR.TAXI_STAGE/dim_date.csv.gz
            FILE_FORMAT = (
                FORMAT_NAME = TAXI_DB.STAR.TAXI_CSV_FORMAT
            );
        """,
    )

    # 3. Load fact table

    load_fact_trips = SQLExecuteQueryOperator(
        task_id="load_fact_trips",
        conn_id="snowflake_conn",
        sql="""
            COPY INTO TAXI_DB.STAR.FACT_TRIPS
            FROM @TAXI_DB.STAR.TAXI_STAGE/fact_trips.csv.gz
            FILE_FORMAT = (
                FORMAT_NAME = TAXI_DB.STAR.TAXI_CSV_FORMAT
            );
        """,
    )

    # Task dependencies

    run_databricks >> [
        load_dim_payment,
        load_dim_rate,
        load_dim_date,
    ]

    [
        load_dim_payment,
        load_dim_rate,
        load_dim_date,
    ] >> load_fact_trips