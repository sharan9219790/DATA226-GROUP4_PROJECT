# **Santa Clara Crash Analytics Pipeline — DATA226 — Group Project**
*(Airflow → Snowflake → dbt → Tableau)*

## 📘 Overview
This project implements a complete, production-oriented **ELT (Extract–Load–Transform)** pipeline designed to automate **traffic accident analytics for Santa Clara County**.

The workflow includes:

1. **Extraction** — ingest historical crash CSVs, weather API data, and traffic API data  
2. **Loading** — store raw datasets in the **Snowflake RAW schema**  
3. **Transformation** — clean and model data using **dbt**  
4. **Visualization** — build analytical dashboards using **Tableau**  

This demonstrates enterprise-level orchestration, warehousing, transformation modeling, and BI integration.

---

## 🧱 Architecture Diagram
```mermaid
flowchart LR
    CSV[Historical Crash Data] --> A[traffic_crash_etl.py\nAirflow DAG]
    WEATHER[OpenWeather API] --> W[weather.py\nAirflow DAG]
    TRAFFIC[Google Distance Matrix API] --> G[Google_maps.py\nAirflow DAG]

    A --> RAW[Snowflake RAW Schema]
    W --> RAW
    G --> RAW

    RAW --> DBT[dbt Models\nStaging → Intermediate → Marts]
    DBT --> MART[Snowflake MART Schema]

    MART --> TABLEAU[Tableau Dashboards]
    TABLEAU --> INSIGHTS[Risk Hotspots\nWeather Impact\nCrash Forecasts]
📁 Repository Structure
bash
Copy code
.
├── dags/
│   ├── Google_maps.py              # Traffic API ingestion (Google Distance Matrix)
│   ├── weather.py                  # Weather API ingestion (OpenWeatherMap)
│   ├── traffic_crash_etl.py        # Crash data ingestion + dbt trigger
│   └── snowflake_connector.py      # Shared Snowflake utilities
│
├── data/                           # Historical accident datasets
├── tableau/                        # Dashboard files / screenshots
├── compose.yaml                    # Docker Compose for Airflow cluster
└── README.md
🔧 Prerequisites
Python 3.10+

Docker + Docker Compose

Snowflake Account

dbt-core + dbt-snowflake

Tableau Desktop / Tableau Public

API keys:

OpenWeatherMap

Google Distance Matrix API

🔐 Required Environment Variables
bash
Copy code
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
🌀 Airflow Configuration
1. Start Airflow
bash
Copy code
docker-compose -f compose.yaml up --build
2. Access Airflow UI
http://localhost:8080
Login: airflow / airflow

3. Configure Snowflake Connection
Airflow → Admin → Connections → snowflake_conn

Fill in:

yaml
Copy code
Conn Type: Snowflake
Account: <account>
User: <user>
Password: <password>
Warehouse: COMPUTE_WH
Database: ACCIDENT_DW
Schema: RAW
Role: DATA226_ROLE
4. Configure Airflow Variables
Variable	Value
snowflake_database	ACCIDENT_DW
raw_schema	RAW
intermediate_schema	INT
mart_schema	MART
openweather_api_key	<key>
traffic_api_key	<key>

📡 DAGs Overview
traffic_crash_etl.py
Loads historical crash CSV data

Performs validation

Writes into RAW.CRASHES

Triggers dbt run

weather.py
Fetches weather metrics from OpenWeatherMap

Stores into RAW.WEATHER tables

Google_maps.py
Fetches travel time + congestion using Google Distance Matrix

Stores into RAW.TRAFFIC tables

snowflake_connector.py
Utility module for executing Snowflake SQL

Handles DDL/DML, connections, and staging

🧱 dbt Layer
Run manually:

bash
Copy code
dbt debug
dbt run
dbt test
Verify results:

sql
Copy code
SELECT COUNT(*) FROM RAW.CRASHES;
SELECT * FROM MART.FACT_CRASHES LIMIT 20;
Final dbt models include:

FACT_CRASHES

DIM_LOCATION

DIM_WEATHER

DIM_TRAFFIC

DIM_DATE

📊 Tableau Dashboard
Connect Tableau → Snowflake:

makefile
Copy code
Warehouse: COMPUTE_WH
Database: ACCIDENT_DW
Schema: MART
Recommended charts:

Crash trends over time

Severity distribution

Weather × traffic control risk heatmaps

Roadway surface & lighting condition effects

Geographic accident hotspots

Crash forecasting

📄 License
For academic use in DATA 226 — San José State University.
