from datetime import datetime

import psycopg2
import snowflake.connector

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable


# ---------- helpers ----------

def get_pg_conn():
    """
    Connect to the Postgres container defined in docker-compose.
    """
    return psycopg2.connect(
        host="postgres",
        dbname="airflow",
        user="airflow",
        password="airflow",
        port=5432,
    )


def sync_crash_summary_to_snowflake():
    """
    1) Read crash severity summary from Postgres (stg_crashes)
    2) Write the aggregated result into Snowflake table CRASH_SEVERITY_SUMMARY
    """

    # 1. Read from Postgres
    pg_conn = get_pg_conn()
    pg_cur = pg_conn.cursor()

    pg_cur.execute(
        """
        SELECT
            severity_category,
            COUNT(*) AS crash_count,
            COALESCE(SUM(total_injuries), 0) AS total_injuries
        FROM stg_crashes
        GROUP BY severity_category
        ORDER BY crash_count DESC;
        """
    )

    rows = pg_cur.fetchall()

    pg_cur.close()
    pg_conn.close()

    if not rows:
        print("No rows found in stg_crashes; nothing to sync to Snowflake.")
        return

    # 2. Connect to Snowflake (credentials from Airflow Variables)
    ctx = snowflake.connector.connect(
        account=Variable.get("SNOWFLAKE_ACCOUNT"),
        user=Variable.get("SNOWFLAKE_USER"),
        password=Variable.get("SNOWFLAKE_PASSWORD"),
        warehouse=Variable.get("SNOWFLAKE_WAREHOUSE"),
        database=Variable.get("SNOWFLAKE_DATABASE"),
        schema=Variable.get("SNOWFLAKE_SCHEMA"),
    )

    cs = ctx.cursor()
    try:
        # Create target table if needed
        cs.execute(
            """
            CREATE TABLE IF NOT EXISTS CRASH_SEVERITY_SUMMARY (
                SEVERITY_CATEGORY STRING,
                CRASH_COUNT NUMBER,
                TOTAL_INJURIES NUMBER
            );
            """
        )

        # Simple strategy: truncate + full reload
        cs.execute("TRUNCATE TABLE CRASH_SEVERITY_SUMMARY")

        # Insert all rows
        cs.executemany(
            """
            INSERT INTO CRASH_SEVERITY_SUMMARY
                (SEVERITY_CATEGORY, CRASH_COUNT, TOTAL_INJURIES)
            VALUES (%s, %s, %s)
            """,
            rows,
        )

        ctx.commit()
        print(f"Synced {len(rows)} summary rows to Snowflake.")
    finally:
        cs.close()
        ctx.close()


# ---------- DAG definition ----------

with DAG(
    dag_id="snowflake_crash_summary_sync",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",   # or "0 2 * * *" for 2am daily
    catchup=False,
    tags=["snowflake", "traffic", "analytics"],
) as dag:

    start = EmptyOperator(task_id="start")

    sync_to_snowflake = PythonOperator(
        task_id="sync_crash_summary_to_snowflake",
        python_callable=sync_crash_summary_to_snowflake,
    )

    end = EmptyOperator(task_id="end")

    start >> sync_to_snowflake >> end
