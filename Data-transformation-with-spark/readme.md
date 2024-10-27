# ETL | Data Transformation using Apache Spark

## 1. Project overview
Data transformation plays a critical process in data-driven organization.
In this project, (E)Transoform(L) process purpose is to conduct viability analysis to gain insight from various of complex data.
Transformation will be use apache spark with python and automate the process using apache airflow.

## 2. Project Objective
<-> Load data from data warehouse - postgres to spark dataFrame via docker container. <br>
<-> Load data from table into parquet type <br>
<-> Perform query using Pyspark SQL to transform employee data and candidate data.  <br>
<-> Save the data into target (data mart - postgres)  <br>

## 3. System Architecture
![Local-Database](https://github.com/vnobets7/final_project_ftde_ricky/blob/ftde-dev/Data-transformation-with-spark/images/SS-System-architecture.PNG)

## 4. Requirements
  - Python 3.10+
  - Docker 18.09+ | local environment
  - Docker compose 1.24+ | local environment
  - Dbeaver | local environment
  - VSCode | local environment
  - Apache spark | local environment
  - PostgreSQL | local environment
  - [Optional] Pgadmin 4
  - [Optional] *nix system

## 5. Running the project
1.Navigate to the project directory
```
cd .\final_project_ftde_ricky\Data-transformation-with-spark\
```	

2.Starts the containers for PostgreSQL
```
docker compose -f docker-compose-postgres.yaml up -f docker-compose-postgres.yaml up -d
```

3.Starts the containers for apache airflow
```
docker compose -f docker-compose-airflow.yaml -f docker-compose-airflow.yaml up --build -d
```

4.Allow docker container to connect to a localhost postgres database [optional] <br>
4a.Find IP Address for postgresql database on docker container
```
docker inspect <docker_container_name>
```
4b.Open pg_hba.conf file and add this to that file if the host IP address range not inside
```
example: host    all             all             172.0.0.0/8            md5
```
4c.Open postgresql.conf file and change the setting if the current set like this listen_addresses = '#'
```
example: listen_addresses = '*'
```
<br>
### table of docker images and services

| Docker Image                         |
|--------------------------------------|
| apache/airflow:latest                |
| mkhasan0007/bitnami-spark:3.1.2      |
| bitnami/spark:3.1.2                  |
| jupyter/pyspark-notebook:spark-3.2.0 |
| postgres:latest                      |

<br>

5.Access jupyter notebook for running pyspark [optional]
5a.Retrieve the access URL for jupyter notebook
```
docker logs $(docker ps -q --filter "ancestor=jupyter/pyspark-notebook:spark-3.2.0") 2>&1 | grep '<IP Address>' | tail -1
```
5b.Insert jupyter notebook token on the browser
<br>

6.Open the data pipline on apache airflow
6a.Access the airflow webserver(airflow UI)
```
open http://localhost:8080 on the browser
```
6b.Type username and password that already been set on docker-compose-sample.yaml or .env file
<br>

7.Stop and remove the docker container (after finish extracting) <br>
7a.Stop and remove the PostgreSQL and Mysql container
```
docker compose -f docker-compose-db.yaml -f docker-compose-db.yaml down --remove-orphans -v
```
7b.Stop and remove the the airflow container
```
(1) logout from airflow UI
(2) docker compose -f docker-compose-airflow.yaml -f docker-compose-airflow.yaml down --remove-orphans -v
```
<br>
- The DAG orchestrates the python operator for scheduler <br>
-> ./airflow/dags/: contains airflow DAG that manage ETL process <br>
-> ./logs/: contains logs from task execution and scheduler <br>
-> ./jobs/: contains custom plugins (optional) <br>
-> ./scripts/: contains custom function/module for the operator on DAG file (optional) <br>
-> ./spark_drivers/: contains postgres driver (optional) <br>

### The DAG preview
![DAG-graph](https://github.com/vnobets7/final_project_ftde_ricky/blob/ftde-dev/Data-transformation-with-spark/images/SS-The-graph-view.PNG)

## 6. Analytics preview

### Distribution of employee ages
![Age-distribution](https://github.com/vnobets7/final_project_ftde_ricky/blob/ftde-dev/Data-transformation-with-spark/images/SS-candidate-based-on-aged.PNG)

### Top 10 employee got high bonus for after work time
![Top-10-high-pay](https://github.com/vnobets7/final_project_ftde_ricky/blob/main/Data-transformation-with-spark/images/SS-Top-10-highest-bonusovertime-title.PNG)

## 7. The Data marts result
![Data-mart](https://github.com/vnobets7/final_project_ftde_ricky/blob/ftde-dev/Data-transformation-with-spark/images/SS-Data-mart-docker.PNG)
