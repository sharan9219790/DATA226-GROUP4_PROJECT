Santa Clara Crash Analytics Pipeline — DATA226 — Group Project
(Airflow → Snowflake → dbt → Tableau)

Overview:
This project implements an ELT pipeline for analyzing Santa Clara County crash data using Airflow, Snowflake, dbt, and Tableau.

Pipeline steps:
1. Extraction — crash CSV, live weather API (OpenWeather), live traffic API (Google Distance Matrix)
2. Loading — write all raw data into the Snowflake RAW schema
3. Transformation — dbt models (staging → intermediate → marts)
4. Visualization — Tableau dashboards for crash trends, hotspots, weather influence, road conditions, and risk forecasting

------------------------------------------------------------
Architecture Diagram (Mermaid)
Paste this into GitHub (outside this code block) to render:

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
    │   ├── Google_maps.py           (traffic API ingestion)
    │   ├── weather.py               (weather API ingestion)
    │   ├── traffic_crash_etl.py     (crash CSV ingestion + dbt trigger)
    │   └── snowflake_connector.py   (shared Snowflake utilities)
    ├── data/                        (historical accident dataset)
    ├── tableau/                     (final dashboards or screenshots)
    ├── compose.yaml                 (Docker Compose for Airflow)
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

2. Airflow UI:

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
DAGs (based on your actual files):

Google_maps.py:
    - Fetches travel-time and congestion metrics from Google Distance Matrix API
    - Loads results into Snowflake RAW.TRAFFIC tables

weather.py:
    - Fetches weather snapshots from OpenWeatherMap API
    - Loads results into RAW.WEATHER tables

traffic_crash_etl.py:
    - Loads crash historical CSV files into RAW.CRASHES
    - Performs cleanup and validation
    - Triggers dbt transformations after ingestion completes

snowflake_connector.py:
    - Shared Snowflake connection utilities
    - Handles table creation, DDL, DML

------------------------------------------------------------
dbt Layer:

Commands:

       dbt debug
       dbt run
       dbt test

Snowflake validation queries:

       SELECT COUNT(*) FROM RAW.CRASHES;
       SELECT * FROM MART.FACT_CRASHES LIMIT 20;

dbt model outputs include:

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

Recommended charts:

       Crash trends by month/year
       Severity breakdown (minor, moderate, severe, fatal)
       Weather × traffic control heatmaps
       Road surface and lighting effect charts
       Geospatial hotspot map
       Crash forecast line charts

------------------------------------------------------------
License:
For academic use in DATA 226 (San José State University).
