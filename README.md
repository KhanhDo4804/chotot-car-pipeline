# An end-to-end data pipeline designed to automatically collect, process, and analyze used car market data from Chotot. The system provides insights to identify “good deals” and analyze depreciation trends of different car models over time.



## System Architecture

The data flow follows the Medallion Architecture:

* **Crawler**: A Python script that extracts raw data from Chotot.
* **Bronze Layer**: Stores raw snapshot data.
* **Silver Layer**: Cleans, standardizes data types, and stores data using Delta Lake (with simulated CDC via change detection).
* **Gold Layer**: Applies complex business logic using PySpark and loads the results into PostgreSQL.
* **Visualization**: Interactive dashboards built with Metabase.


## Tech Stack

* **Orchestration**: Apache Airflow
* **Processing**: PySpark (Spark SQL & DataFrames)
* **Storage**: Delta Lake, PostgreSQL
* **Visualization**: Metabase
* **Infrastructure**: Docker & Docker Compose
!(images/Architecture.png)
---

## FeBusiness Logic

Based on the Gold layer transformations:

* **Deal Finder**: Automatically compares listing prices against the market median and labels cars priced 15% below the median as “Great Deal”.

* **Depreciation Analysis**: Calculates the annual depreciation rate (%) for each car model.

* **Regional Trends**: Ranks the most popular car models by province across Vietnam.

* **Recommendation System**: Scores vehicles based on usage intensity (km/year) and value within a given budget segment.


## Dashboard Preview

!(images/Visualization.png)



## How to Run

### 1. Clone the repository

```bash
git clone https://github.com/KhanhDo4804/chotot-used-car-data-pipeline.git
```

### 2. Start the system with Docker

```bash
docker-compose up -d
```

### 3. Access services

* **Airflow**: http://localhost:8080 → trigger DAGs
!(images/Airflow.png)
* **Metabase**: http://localhost:3001 → connect to PostgreSQL and view dashboards
