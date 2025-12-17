Santa Clara Crash Analytics Pipeline — DATA226 — Group Project
(Airflow → Snowflake → dbt → Tableau)

Overview:
    This project implements a full ELT pipeline for analyzing Santa Clara County crash data.
    It integrates historical crash CSVs, traffic APIs, weather APIs, Snowflake warehousing,
    dbt transformations, and Tableau dashboards.

Pipeline Steps:
    1. Extraction — crash CSV, OpenWeather API, Google Distance Matrix API
    2. Loading — Snowflake RAW schema
    3. Transformation — dbt staging → intermediate → marts
    4. Visualization — Tableau dashboards

------------------------------------------------------------
Architecture Diagram (Mermaid)

    mermaid
    flowchart LR
        CSV[Historical Crash Data] --> A[traffic_crash_etl.py Airflow DAG]
        WEATHER[OpenWeather API] --> W[weather.py Airflow DAG]
        TRAFFIC[Google Distance Matrix API] --> G[Google_maps.py Airflow DAG]
        A --> RAW[Snowflake RAW Schema]
        W --> RAW
        G --> RAW
        RAW --> DBT[dbt Models]
        DBT --> MART[Snowflake MART Schema]
        MART --> TABLEAU[Tableau Dashboards]
        TABLEAU --> INSIGHTS[Risk Hotspots, Weather Impact, Crash Forecasts]

------------------------------------------------------------
Repository Structure:

    .
    ├── dags/
    │   ├── Google_maps.py          (traffic API ingestion)
    │   ├── weather.py              (weather API ingestion)
    │   ├── traffic_crash_etl.py    (crash ETL + dbt trigger)
    │   └── snowflake_connector.py   (shared Snowflake utilities)
    ├── data/
    ├── tableau/
    ├── compose.yaml
    └── README.md

------------------------------------------------------------
Prerequisites:

    Python 3.10+
    Docker + Docker Compose
    Snowflake account
    dbt-core + dbt-snowflake
    Tableau Desktop / Public
    API keys: OpenWeatherMap, Google Distance Matrix

------------------------------------------------------------
Required Environment Variables:

    export SNOWFLAKE_ACCOUNT="<account>"
    export SNOWFLAKE_USER="<user>"
    export SNOWFLAKE_PASSWORD="<password>"
    export SNOWFLAKE_ROLE="DATA226_ROLE"
    export SNOWFLAKE_WAREHOUSE="COMPUTE_WH"
    export SNOWFLAKE_DATABASE="ACCIDENT_DW"
    export SNOWFLAKE_SCHEMA="RAW"

    export OPENWEATHER_API_KEY="<weather_key>"
    export GOOGLE_DISTANCE_MATRIX_API_KEY="<maps_key>"

    export DBT_PROFILES_DIR="$(pwd)/dbt"
    export AIRFLOW_HOME="$(pwd)/.airflow"

------------------------------------------------------------
Airflow Configuration:

1. Start Airflow:
        docker-compose -f compose.yaml up --build

2. Access Airflow UI:
        http://localhost:8080
        username: airflow
        password: airflow

3. Snowflake Connection (snowflake_conn):
        Conn Type: Snowflake
        Account: <account>
        User: <user>
        Password: <password>
         Warehouse: COMPUTE_WH
        Database: ACCIDENT_DW
        Schema: RAW
        Role: DATA226_ROLE

4. Airflow Variables:
        snowflake_database = ACCIDENT_DW
        raw_schema = RAW
        intermediate_schema = INT
        mart_schema = MART
        openweather_api_key = <key>
        traffic_api_key = <key>

------------------------------------------------------------
DAGs (matching your actual files):

Google_maps.py:
    Fetches Google Distance Matrix travel time + congestion
    Loads into RAW.TRAFFIC tables

weather.py:
    Fetches OpenWeatherMap data
    Loads into RAW.WEATHER tables

traffic_crash_etl.py:
    Loads crash CSV → RAW.CRASHES
    Cleans + validates data
    Triggers dbt run

snowflake_connector.py:
    Helper module for all Snowflake operations

------------------------------------------------------------
dbt Layer:

Run dbt:
        dbt debug
        dbt run
        dbt test

Snowflake test queries:
        SELECT COUNT(*) FROM RAW.CRASHES;
        SELECT * FROM MART.FACT_CRASHES LIMIT 20;

dbt output tables:
        FACT_CRASHES
        DIM_LOCATION
        DIM_WEATHER
        DIM_TRAFFIC
        DIM_DATE

------------------------------------------------------------
Tableau Dashboard:

Snowflake Connection:
        Warehouse: COMPUTE_WH
        Database: ACCIDENT_DW
        Schema: MART

Dashboard visuals:
        Crash trends
        Severity distribution
        Weather × traffic control risk
        Road surface & lighting impact
        Geospatial hotspots
        Crash forecasting

------------------------------------------------------------
License:
    For academic use in DATA 226 (San José State University)
