# ETL | Batch - Data Processing

## 1. Project overview
In this project, ETL Data Pipeline - batch will use apache airflow to extract employee data and candidate data from multiple csv file, and then load them in 2 different database(PostgreSQL+Mysql).
All data processing is running from local computer on docker container with the help of docker desktop + WSL2(windows OS).

## 2. Dataset
There 3 different type of data in this project:
- Data management and payroll from PostgreSQL (batch processing)
- Training and development from MySQL (batch processing)
- Performance and management from PostgreSQL (batch processing)
<br>
Our data source consist of 3 tables. <br>
<-> csv1: data_management_payroll
- employeeid
- name
- gender
- age 
- department
- position
- salary
- overtimePay
- paymentDate
<br>
<-> csv2: data_performance_management
- employeeid
- name
- gender
- reviewPeriod
- rating
- comments
<br>
<-> csv3: data_training_development
- employeeid
- name
- trainingProgram
- startDate
- endDate
- status
<br>

## 3. System architecture
![Sytem-arch](https://github.com/vnobets7/final_project_ftde_ricky/blob/ftde-dev/Data-ingestion/data-batch-processing/images/SS-system-architecture.PNG)

## 4. Tech Stack
- Docker (local version)
- Docker compose (local version)
- Python (local version)
- PostgreSQL (local version)
- Apache Airflow (local version)

## 5. Project workflow

### Installation and setup
1.Navigate to the project directory
```
cd .\final_project_ftde_ricky\Data-ingestion\data-batch-processing\
```	

2.Starts the containers for PostgreSQL and Mysql in the background
```
docker compose -f docker-compose-db.yaml up -f docker-compose-db.yaml up -d
```

3.Starts the containers for apache airflow
```
docker compose -f docker-compose-airflow.yaml -f docker-compose-airflow.yaml up --build -d
```

4.Allow docker container to connect to a localhost postgres database [optional]
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

5.Open the data pipline on apache airflow
5a.Access the airflow webserver(airflow UI)
```
open http://localhost:8080 on the browser
```
5b.Type username and password that already been set on docker-compose-sample.yaml or .env file
<br>

6.Run the orchestration
6a.Click the DAG name "dag1-data-ingestion" on DAG waiting list
<br>
6b.Run the DAG
<br>

7.Stop and remove the docker container (after finish extracting)
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
-> ./airflow/dags/data_dump/: csv file as data sources

## 6. The DAG preview
![DAG-graph](https://github.com/vnobets7/final_project_ftde_ricky/blob/main/Data-ingestion/data-batch-processing/images/SS-The-graph-view-new.PNG)

### Airflow main page
![DAG-airflow](https://github.com/vnobets7/final_project_ftde_ricky/blob/main/Data-ingestion/data-batch-processing/images/SS-The-airflow-overview.PNG)

## 7. Database result
- Staging-db: performance_management <br>
- Staging-db: training_development <br>
- Staging-db: management_payroll <br>
![data-staging-db](https://github.com/vnobets7/final_project_ftde_ricky/blob/main/Data-ingestion/data-batch-processing/images/SS-Staging-db-all-data.PNG)
