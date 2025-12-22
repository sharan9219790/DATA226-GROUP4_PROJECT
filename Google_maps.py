from datetime import datetime
import json
import requests
import psycopg2

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable


# --- helpers --------------------------------------------------------------

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


def fetch_and_store_google_maps():
    """
    Fetch live travel times from the Google Maps Directions API for a set
    of important road segments in Santa Clara County and store them in
    a raw_traffic table.
    """

    # API key is stored securely as an Airflow Variable
    api_key = Variable.get("GOOGLE_MAPS_API_KEY")

    # List of segments is stored as JSON in an Airflow Variable
    # Example value:
    # [
    #   {"segment_id": "sj_downtown",
    #    "origin": "37.3382,-121.8863",
    #    "destination": "37.3541,-121.9552"}
    # ]
    locations_json = Variable.get(
        "GOOGLE_MAPS_LOCATIONS",
        default_var=json.dumps([
            {
                "segment_id": "sj_downtown",
                "origin": "37.3382,-121.8863",
                "destination": "37.3541,-121.9552",
            }
        ]),
    )
    locations = json.loads(locations_json)

    conn = get_pg_conn()
    cur = conn.cursor()

    # Create raw_traffic table if it doesn't exist
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_google_traffic (
            id SERIAL PRIMARY KEY,
            segment_id TEXT,
            origin TEXT,
            destination TEXT,
            distance_m INTEGER,
            duration_s INTEGER,
            status TEXT,
            fetched_at TIMESTAMP DEFAULT NOW()
        );
        """
    )

    for loc in locations:
        params = {
            "origin": loc["origin"],
            "destination": loc["destination"],
            "departure_time": "now",
            "key": api_key,
        }

        response = requests.get(
            "https://maps.googleapis.com/maps/api/directions/json",
            params=params,
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()

        status = data.get("status")
        if status != "OK":
            # Skip segments where the API didn't return a valid route
            continue

        leg = data["routes"][0]["legs"][0]
        distance_m = leg["distance"]["value"]
        duration_s = leg["duration"]["value"]

        cur.execute(
            """
            INSERT INTO raw_google_traffic (
                segment_id, origin, destination,
                distance_m, duration_s, status
            )
            VALUES (%s, %s, %s, %s, %s, %s);
            """,
            (
                loc["segment_id"],
                loc["origin"],
                loc["destination"],
                distance_m,
                duration_s,
                status,
            ),
        )

    conn.commit()
    cur.close()
    conn.close()


# --- DAG definition -------------------------------------------------------

with DAG(
    dag_id="google_maps_traffic_etl",
    start_date=datetime(2024, 1, 1),
    schedule="*/30 * * * *",  # every 30 minutes (change as you like)
    catchup=False,
    tags=["google_maps", "traffic"],
) as dag:

    start = EmptyOperator(task_id="start")

    fetch_traffic = PythonOperator(
        task_id="fetch_google_maps_traffic",
        python_callable=fetch_and_store_google_maps,
    )

    end = EmptyOperator(task_id="end")

    start >> fetch_traffic >> end
