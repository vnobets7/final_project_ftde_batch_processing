# ETL | Data Warehouse Design

## 1. Project overview
Build a data warehouse to load data from 2 different data process(batch processing and stream processing).
In this project, it will be use 'snowflake schema' for data warehouse design.
Extraction from staging db into DWH will be conducted using python and sql.

## 2. Project Objective
To-do list:
- Move the data using Python and SQL 
- Create data warehouse modeling with snowflake schema
- Develop DWH using two type of database,(1) On-premise using postegreSQL (2) Cloud Database using Google Cloud SQL - postgres instance <br>
<-> Data Source - staging-db: batch processing + stream processing <br>
<-> Extract/Load: python + sql + docker-compose <br>
<-> Destination - DWH: postgreSQL

## 3. System Architecture
![System-arch](https://github.com/vnobets7/final_project_ftde_ricky/blob/main/Data-warehouse-design/images/SS-system-architecture.PNG)

## 4. Tools use
- Google Cloud SQL - Postgres Instance (cloud version)
- Python (local version)
- Apache airflow (local version)
- Dbeaver (DB GUI)  (local version)
- DBDiagram.io (ERD)  (cloud version)

## 5. Create table
In this project, it will need to use 2 different database (local and cloud for development)
- Local development setup: postegreSQL with docker-compose <br>
<-> Navigate to the project directory
```
cd .\final_project_ftde_ricky\data-ingestion\Data-warehouse-design\
```
<-> Starts the containers for apache airflow in the background
```
docker compose -f docker-compose-airflow.yaml -f docker-compose-airflow.yaml up --build -d
```
<-> Starts the containers for postgreSQL(data warehouse & data mart) in the background
```
docker compose -f docker-compose-db.yaml up -f docker-compose-db.yaml up -d
```
<-> Open pg_hba.conf file and add this to that file if the host IP address range not inside
```
example: host    all             all             172.0.0.0/8            md5
```
<-> Open postgresql.conf file and change the setting if the current set like this listen_addresses = '#'
```
example: listen_addresses = '*'
```
<-> Open the data pipline on apache airflow 5a.Access the airflow webserver(airflow UI)
```
open http://localhost:8080 on the browser
```
<-> Type username and password that already been set on docker-compose-sample.yaml or .env file <br>
<-> Run the orchestration  <br>
<-> Click the DAG name "dag1-data-ingestion" on DAG waiting list <br>
<-> Run the DAG <br>
<-> Stop and remove the docker container (after finish extracting) <br>
<-> Stop and remove the PostgreSQL and Mysql container
```
docker compose -f docker-compose-db.yaml -f docker-compose-db.yaml down --remove-orphans -v
```
<-> Stop and remove the the airflow container
```
(1) logout from airflow UI
(2) docker compose -f docker-compose-airflow.yaml -f docker-compose-airflow.yaml down --remove-orphans -v
```
<-> Cloud development setup using google cloud sql - postgres instance(comming soon)

## 6. The DAG Preview
![airflow-DAG](https://github.com/vnobets7/final_project_ftde_ricky/blob/main/Data-warehouse-design/images/SS-The-graph.PNG)

### Airflow main page
![airflow-mainpage](https://github.com/vnobets7/final_project_ftde_ricky/blob/main/Data-warehouse-design/images/SS-The-airflow-overview.PNG)

## 7. Data Warehouse Design
- staging-db contain data from batch processing & stream processing
![staging-db-docker](https://github.com/vnobets7/final_project_ftde_ricky/blob/main/Data-warehouse-design/images/data-staging-dbeaver-batch-processing.PNG)
<br>
- Data ingestion process from staging-db to DWH_HR
The purpose of creating data warehouse was to enable organization to easily combine data from staging area and store in data warehouse schema.
In this project, DWH create by using snowflake schema type for break down dimension tables into logical subdimension to reduce redundancy and improve data integrity.
![dwh-docker](https://github.com/vnobets7/final_project_ftde_ricky/blob/main/Data-warehouse-design/images/SS-DWH-docker.PNG)

### DWH Detail
<-> All the fact and dim table inside DWH_HR
![DWH-docker](https://github.com/vnobets7/final_project_ftde_ricky/blob/main/Data-warehouse-design/images/SS-final-project-ERD-4-3.PNG)
<br>
<-> Data Warehouse on Goggle Cloud SQL - Postgres instance (comming soon)
