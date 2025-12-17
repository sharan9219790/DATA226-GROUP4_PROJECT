from datetime import datetime
import csv

import psycopg2
from psycopg2.extras import execute_values

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}


def get_pg_conn():
    """
    Connect directly to the Postgres service defined in docker-compose.
    """
    conn = psycopg2.connect(
        host="postgres",
        dbname="airflow",
        user="airflow",
        password="airflow",
        port=5432,
    )
    return conn


def create_raw_table():
    """
    Create the raw_crashes table if it doesn't exist.
    """
    sql = """
    CREATE TABLE IF NOT EXISTS raw_crashes (
        crash_fact_id INTEGER PRIMARY KEY,
        crash_datetime TIMESTAMP,
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION,
        minor_injuries INTEGER,
        moderate_injuries INTEGER,
        severe_injuries INTEGER,
        fatal_injuries INTEGER,
        weather VARCHAR(100),
        lighting VARCHAR(100),
        roadway_surface VARCHAR(100),
        roadway_condition VARCHAR(100),
        primary_collision_factor VARCHAR(255),
        traffic_control VARCHAR(100),
        collision_type VARCHAR(100),
        proximity_to_intersection VARCHAR(100),
        source_file VARCHAR(50)
    );
    """

    conn = get_pg_conn()
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()


def _load_csv_to_postgres(filename: str, source_label: str, **context):
    """
    Simple ETL:
    - Read CSV from /opt/airflow/data/<filename>
    - Insert rows into raw_crashes table in Postgres
    """

    conn = get_pg_conn()
    cursor = conn.cursor()

    file_path = f"/opt/airflow/data/{filename}"
    print(f"Loading file: {file_path}")

    rows = []

    def to_float(val):
        try:
            return float(val) if val not in (None, "", "NULL") else None
        except ValueError:
            return None

    def to_int(val):
        try:
            return int(val) if val not in (None, "", "NULL") else None
        except ValueError:
            return None

    with open(file_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            crash_fact_id_raw = row.get("CrashFactId")
            if not crash_fact_id_raw:
                continue

            crash_fact_id = int(crash_fact_id_raw)
            crash_datetime = row.get("CrashDateTime") or None

            latitude = to_float(row.get("Latitude"))
            longitude = to_float(row.get("Longitude"))

            minor = to_int(row.get("MinorInjuries"))
            moderate = to_int(row.get("ModerateInjuries"))
            severe = to_int(row.get("SevereInjuries"))
            fatal = to_int(row.get("FatalInjuries"))

            weather = row.get("Weather")
            lighting = row.get("Lighting")
            roadway_surface = row.get("RoadwaySurface")
            roadway_condition = row.get("RoadwayCondition")
            primary_factor = row.get("PrimaryCollisionFactor")
            traffic_control = row.get("TrafficControl")
            collision_type = row.get("CollisionType")
            proximity = row.get("ProximityToIntersection")

            rows.append(
                (
                    crash_fact_id,
                    crash_datetime,
                    latitude,
                    longitude,
                    minor,
                    moderate,
                    severe,
                    fatal,
                    weather,
                    lighting,
                    roadway_surface,
                    roadway_condition,
                    primary_factor,
                    traffic_control,
                    collision_type,
                    proximity,
                    source_label,
                )
            )

    if not rows:
        print(f"No rows found in {file_path}")
        cursor.close()
        conn.close()
        return

    insert_sql = """
        INSERT INTO raw_crashes (
            crash_fact_id,
            crash_datetime,
            latitude,
            longitude,
            minor_injuries,
            moderate_injuries,
            severe_injuries,
            fatal_injuries,
            weather,
            lighting,
            roadway_surface,
            roadway_condition,
            primary_collision_factor,
            traffic_control,
            collision_type,
            proximity_to_intersection,
            source_file
        )
        VALUES %s
        ON CONFLICT (crash_fact_id) DO NOTHING;
    """

    execute_values(cursor, insert_sql, rows, page_size=1000)
    conn.commit()
    cursor.close()
    conn.close()

    print(f"Inserted {len(rows)} rows from {filename}")

def transform_raw_to_staging():
    """
    Simple transform step:
    - Build a staging table stg_crashes from raw_crashes
    - Example: compute total_injuries and a severity_category
    """
    sql_drop = "DROP TABLE IF EXISTS stg_crashes;"

    sql_create = """
    CREATE TABLE stg_crashes AS
    SELECT
        crash_fact_id,
        crash_datetime,
        latitude,
        longitude,
        minor_injuries,
        moderate_injuries,
        severe_injuries,
        fatal_injuries,
        (COALESCE(minor_injuries, 0)
         + COALESCE(moderate_injuries, 0)
         + COALESCE(severe_injuries, 0)
         + COALESCE(fatal_injuries, 0)) AS total_injuries,
        CASE
            WHEN fatal_injuries > 0 THEN 'Fatal'
            WHEN severe_injuries > 0 THEN 'Severe'
            WHEN moderate_injuries > 0 THEN 'Moderate'
            WHEN minor_injuries > 0 THEN 'Minor'
            ELSE 'No Injury'
        END AS severity_category,
        weather,
        lighting,
        roadway_surface,
        roadway_condition,
        primary_collision_factor,
        traffic_control,
        collision_type,
        proximity_to_intersection,
        source_file
    FROM raw_crashes;
    """

    conn = get_pg_conn()
    cur = conn.cursor()
    cur.execute(sql_drop)
    cur.execute(sql_create)
    conn.commit()
    cur.close()
    conn.close()



with DAG(
    dag_id="traffic_crash_etl",
    default_args=DEFAULT_ARGS,
    description="Load Santa Clara crash CSVs into raw_crashes table",
    schedule=None,  # run manually for now
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["traffic", "etl"],
) as dag:

    start = EmptyOperator(task_id="start")

    create_raw_table_task = PythonOperator(
        task_id="create_raw_crashes_table",
        python_callable=create_raw_table,
    )

    load_2011_2021 = PythonOperator(
        task_id="load_crashdata_2011_2021",
        python_callable=_load_csv_to_postgres,
        op_kwargs={
            "filename": "crashdata2011-2021.csv",
            "source_label": "2011-2021",
        },
    )

    load_2022_present = PythonOperator(
        task_id="load_crashdata_2022_present",
        python_callable=_load_csv_to_postgres,
        op_kwargs={
            "filename": "crashdata2022-present.csv",
            "source_label": "2022-present",
        },
    )
    transform_crashes = PythonOperator(
        task_id="transform_raw_to_staging",
        python_callable=transform_raw_to_staging,
    )


    end = EmptyOperator(task_id="end")

    start >> create_raw_table_task >> [load_2011_2021, load_2022_present] >> transform_crashes >> end


