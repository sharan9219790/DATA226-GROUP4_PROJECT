from datetime import datetime
import json
import requests
import psycopg2

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


def fetch_and_store_weather():
    """
    Fetch current weather for key locations and store to raw_weather.
    Designed for OpenWeather OneCall / current weather style APIs.
    """

    api_key = Variable.get("WEATHER_API_KEY")

    # List of locations as JSON in an Airflow Variable.
    # Example value:
    # [
    #   {"location_id": "sj_downtown", "lat": 37.3382, "lon": -121.8863},
    #   {"location_id": "santa_clara", "lat": 37.3541, "lon": -121.9552}
    # ]
    locations_json = Variable.get(
        "WEATHER_LOCATIONS",
        default_var=json.dumps([
            {
                "location_id": "sj_downtown",
                "lat": 37.3382,
                "lon": -121.8863,
            }
        ]),
    )
    locations = json.loads(locations_json)

    conn = get_pg_conn()
    cur = conn.cursor()

    # Create raw_weather table if needed
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_weather (
            id SERIAL PRIMARY KEY,
            location_id TEXT,
            lat DOUBLE PRECISION,
            lon DOUBLE PRECISION,
            temp_c DOUBLE PRECISION,
            feels_like_c DOUBLE PRECISION,
            humidity INTEGER,
            pressure INTEGER,
            wind_speed DOUBLE PRECISION,
            weather_main TEXT,
            weather_desc TEXT,
            precipitation_mm DOUBLE PRECISION,
            visibility_m INTEGER,
            fetched_at TIMESTAMP DEFAULT NOW()
        );
        """
    )

    for loc in locations:
        lat = loc["lat"]
        lon = loc["lon"]

        # OpenWeather "Current weather" style endpoint
        params = {
            "lat": lat,
            "lon": lon,
            "appid": api_key,
            "units": "metric",  # Celsius
        }

        response = requests.get(
            "https://api.openweathermap.org/data/2.5/weather",
            params=params,
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()

        main = data.get("main", {})
        wind = data.get("wind", {})
        weather_list = data.get("weather", [])
        weather = weather_list[0] if weather_list else {}

        # Precipitation may come from "rain" or "snow" blocks
        rain = data.get("rain", {})
        snow = data.get("snow", {})
        precip_mm = 0.0
        if "1h" in rain:
            precip_mm += rain["1h"]
        if "1h" in snow:
            precip_mm += snow["1h"]

        cur.execute(
            """
            INSERT INTO raw_weather (
                location_id, lat, lon,
                temp_c, feels_like_c,
                humidity, pressure,
                wind_speed,
                weather_main, weather_desc,
                precipitation_mm,
                visibility_m
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """,
            (
                loc["location_id"],
                lat,
                lon,
                main.get("temp"),
                main.get("feels_like"),
                main.get("humidity"),
                main.get("pressure"),
                wind.get("speed"),
                weather.get("main"),
                weather.get("description"),
                precip_mm,
                data.get("visibility"),
            ),
        )

    conn.commit()
    cur.close()
    conn.close()


# ---------- DAG definition ----------

with DAG(
    dag_id="weather_etl",
    start_date=datetime(2024, 1, 1),
    schedule="*/30 * * * *",  # every 30 minutes
    catchup=False,
    tags=["weather", "traffic"],
) as dag:

    start = EmptyOperator(task_id="start")

    fetch_weather = PythonOperator(
        task_id="fetch_weather_snapshots",
        python_callable=fetch_and_store_weather,
    )

    end = EmptyOperator(task_id="end")

    start >> fetch_weather >> end
