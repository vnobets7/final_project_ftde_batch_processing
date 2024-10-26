# ETL | Data Warehouse Design

## 1. Project overview
Build a data warehouse to load data from 2 different data process(batch processing and stream processing).
In this project, it will be use 'snowflake schema' for data warehouse design.
Extraction from staging db into DWH will be conducted using python and sql.

## 2. Project Objective
<-> To-do list:
- Move the data using Python and SQL 
- Create data warehouse modeling with snowflake schema
- Develop DWH using two type of database,(1) On-premise using postegreSQL (2) Cloud Database using Google Cloud SQL - postgres instance
<-> Data Source - staging-db: batch processing + stream processing
<-> Extract/Load: python + sql + docker-compose
<-> Destination - DWH: postgreSQL

## 3. Dataflow Architecture
![System-arch](https://github.com/vnobets7/final_project_ftde_ricky/blob/ftde-dev/Data-warehouse-design/images/SS-system-architecture.PNG)

## 4. Tools use
- Google Cloud SQL - Postgres Instance (cloud version)
- Python (local version)
- Apache airflow (local version)
- Dbeaver (DB GUI)  (local version)
- DBDiagram.io (ERD)  (cloud version)

## 5. Create table
In this project, it will need to use 2 different database (local and cloud for development)
- Local development setup: postegreSQL with docker-compose
<-> Navigate to the project directory
```
cd .\final_project_ftde_ricky\data-ingestion\Data-warehouse-design\
```
<-> Use docker container from folder ./data-ingestion/data-stream-processing
```
$ docker-compose up -d
```
<-> Navigate to the project directory
```
$ cd ./data-warehouse-design
```
<-> Activate ananconda environment on local computer
```
$ conda activate <insert_name>
```
<-> Install all necessary python library on requirements.txt
```
$ pip install -r requirements. txt
```
<-> Run main.py to create data warehouse schema and move the data from staging to data Warehouse
```
$ python main.py
```
<-> Cloud development setup using google cloud sql - postgres instance(comming soon)

### The DAG Preview
![airflow-DAG](https://github.com/vnobets7/final_project_ftde_ricky/blob/ftde-dev/Data-warehouse-design/images/SS-The-graph.PNG)

## 6. Data Warehouse Design
- staging-db contain data from batch processing & stream processing
This ERD of fact and dimensional table in HR data warehouse
![staging-db](https://github.com/vnobets7/final_project_ftde_ricky/blob/ftde-dev/Data-warehouse-design/images/data-staging-dbeaver-batch-processing.PNG)
<br>
- Data ingestion process from staging-db to DWH_HR
The purpose of creating data warehouse was to enable organization to easily combine data from staging area and store in data warehouse schema.
In this project, DWH create by using snowflake schema type for break down dimension tables into logical subdimension to reduce redundancy and improve data integrity.
![python-code](https://github.com/vnobets7/final_project_ftde_ricky/blob/ftde-dev/Data-warehouse-design/images/SS-final-project-23.PNG)

### DWH Detail
<-> All the fact and dim table inside DWH_HR
![DWH-dbeaver](https://github.com/vnobets7/final_project_ftde_ricky/blob/ftde-dev/Data-warehouse-design/images/SS-final-project-ERD-4-3.PNG)
<br>
![DWH-docker](https://github.com/vnobets7/final_project_ftde_ricky/blob/ftde-dev/Data-warehouse-design/images/SS-DWH-docker.PNG)
<br>
<-> Data Warehouse on Goggle Cloud SQL - Postgres instance (comming soon)
