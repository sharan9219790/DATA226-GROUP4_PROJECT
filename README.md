# Santa Clara Crash Analytics Pipeline — DATA226 - Group project
(Airflow → Snowflake → dbt → Tableau)

## 📘 Overview
This project implements an ELT (Extract–Load–Transform) pipeline for analyzing traffic accident data from Santa Clara County using Airflow, Snowflake, dbt, and Tableau.

Pipeline steps:
1. Extraction — historical crash CSV, live weather API, live traffic API  
2. Loading — write raw data into Snowflake RAW schema  
3. Transformation — dbt models (staging → intermediate → marts)  
4. Visualization — Tableau dashboards for trends, hotspots, and risk analysis  

---

## 📁 Repository Structure

    .
    ├── dags/
    │   ├── Google_maps.py           # DAG for Google Distance Matrix (traffic)
    │   ├── weather.py               # DAG for OpenWeatherMap (weather)
    │   ├── traffic_crash_etl.py     # Main crash ETL DAG (loads CSV, runs dbt)
    │   └── snowflake_connector.py   # Shared Snowflake connection / utilities
    ├── data/                        # Historical accident dataset(s)
    ├── tableau/                     # Tableau dashboards / screenshots
    ├── compose.yaml                 # Docker Compose for Airflow stack
    └── README.md
    |___ dbt                         #dbt
---

## 🔧 Prerequisites

- Python 3.10+  
- Docker & Docker Compose  
- Snowflake account  
- dbt-core + dbt-snowflake  
- Tableau Desktop or Tableau Public  
- API keys:
  - OpenWeatherMap  
  - Google Distance Matrix API  

---

## 🔐 Required Environment Variables

    export SNOWFLAKE_ACCOUNT="<account>"
    export SNOWFLAKE_USER="<user>"
    export SNOWFLAKE_PASSWORD="<password>"
    export SNOWFLAKE_ROLE="ROLE"
    export SNOWFLAKE_WAREHOUSE="COMPUTE_WH"
    export SNOWFLAKE_DATABASE="ACCIDENT_DW"
    export SNOWFLAKE_SCHEMA="RAW"

    export OPENWEATHER_API_KEY="<weather_key>"
    export GOOGLE_DISTANCE_MATRIX_API_KEY="<maps_key>"

    export DBT_PROFILES_DIR="$(pwd)/dbt"
    export AIRFLOW_HOME="$(pwd)/.airflow"

---

## 🌀 Airflow Configuration

### 1. Start Airflow with Docker Compose

    docker-compose -f compose.yaml up --build

### 2. Airflow UI

    http://localhost:8080
    username: airflow
    password: airflow

### 3. Snowflake Connection (snowflake_conn)

    Conn Type: Snowflake
    Account: <account>
    User: <user>
    Password: <password>
    Warehouse: COMPUTE_WH
    Database: ACCIDENT_DW
    Schema: RAW
    Role: DATA226_ROLE

### 4. Airflow Variables

    snowflake_database      = ACCIDENT_DW
    raw_schema              = RAW
    intermediate_schema     = INT
    mart_schema             = MART
    openweather_api_key     = <key>
    traffic_api_key         = <key>

---

## 📡 DAGs (by file)

### Google_maps.py
- Airflow DAG to call **Google Distance Matrix API**
- Fetches travel time / congestion for configured origin–destination pairs
- Writes raw traffic data into `RAW.TRAFFIC_*` tables in Snowflake

### weather.py
- Airflow DAG to call **OpenWeatherMap API**
- Fetches current weather for relevant locations / time ranges
- Writes raw weather data into `RAW.WEATHER_*` tables in Snowflake

### traffic_crash_etl.py
- Main **crash ETL DAG**
- Reads crash CSV files from `data/`
- Uses `snowflake_connector.py` to load into `RAW.CRASHES`
- Triggers dbt (staging → intermediate → marts) once loads succeed

### snowflake_connector.py
- Shared utility module used by the DAGs
- Manages Snowflake connections, queries, and table creation
- Encapsulates common DDL/DML used by ETL tasks

---

## 🧱 dbt Layer

Example manual dbt commands (inside your dbt project):

    cd dbt
    dbt debug
    dbt run
    dbt test

Example checks in Snowflake:

    SELECT COUNT(*) FROM RAW.CRASHES;
    SELECT * FROM MART.FACT_CRASHES ORDER BY CRASH_DATE DESC LIMIT 20;

dbt models typically include:

- Staging models: cleaned versions of `RAW` tables  
- Intermediate models: crash joined with weather + traffic  
- Mart models:  
    - `FACT_CRASHES`  
    - `DIM_DATE`  
    - `DIM_LOCATION`  
    - `DIM_WEATHER`  
    - `DIM_TRAFFIC`  

---

## 📊 Tableau Dashboard

Snowflake connection settings for Tableau:

    Warehouse: COMPUTE_WH
    Database: ACCIDENT_DW
    Schema: MART

Recommended charts:

- Crashes by month / year  
- Severity distribution (minor, moderate, severe, fatal)  
- Collision type breakdown  
- Weather vs. traffic control heatmap  
- Road surface and lighting condition impacts  
- Geospatial accident hotspots (map)  
- Simple crash forecast over time  

Combine these into a single **Accident Analytics Dashboard** for your presentation.

---

## 📄 License

For educational use in **DATA 226 — Data Warehousing** (San José State University).
