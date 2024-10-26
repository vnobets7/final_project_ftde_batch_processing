# ETL | Stream - Data Processing

## 1. Project overview
In this project, ETL Data Pipeline - stream will use apache kafka with help of confluent cloud that consume real-time data, and then load the data into mongoDB.
After that, use that data for testing and train by Machine Learning(linear regression) for prediction.
Finally, save all the data and prediction result to Data Warehouse postgreSQL .
A simple example that takes .csv documents from the `candidate` topic and stores them into the `recruitment-selection-cluster` mongoDB.
The MongoDB - Kafka Source Connector also publishes all change stream events from `candidate` topic into the `recruitment-selection-cluster` mongoDB using python.

## 2. Requirements
  - Python 3.10+
  - Docker 18.09+ | local environment
  - Docker compose 1.24+ | local environment
  - Dbeaver | local environment
  - VSCode | local environment
  - Confluent account (free trial) | cloud environment
  - MongoDB atlas (free trial) | cloud environment
  - [Optional] MySQL workbench
  - [Optional] Pgadmin 4
  - [Optional] Confluent CLI
  - [Optional] Offset Explorer
  - [Optional] Mongo compass
  - [Optional] *nix system

## 3. System architecture
![System-arch](https://github.com/vnobets7/final_project_ftde_ricky/blob/ftde-dev/Data-ingestion/data-stream-processing/images/Stream-processing-dataflow.PNG)

## 4. Dataset
This project using 1 csv as data source: data recruitment selection 

<-> table4: data_training_development
- employeeid
- name
- trainingProgram
- startDate
- endDate
- status

## 5. Running the project
1.Navigate to the project directory
```
cd .\final_project_ftde_ricky\Data-ingestion\data-stream-processing\
```	
2.Starts the containers
```
docker-compose up -d
```
<br>
### table of docker images and services
| Docker Image | 
|--------------|
| apache/airflow:latest |
| confluentinc/cp-zookeeper |
| confluentinc/cp-kafka |
| mongoDB |
| mongo-express |
| postgres:latest |
| postgres:latest |
<br>

3.Starts the containers for apache airflow
```
docker compose -f docker-compose-airflow.yaml -f docker-compose-airflow.yaml up --build -d
```
4.Create new environment on confluent cloud
5.Create new cluster on confluent cloud
6.Generate new API Keys for cluster
7.Create new topics on confluent cloud
8.Copy Bootstrap server + API Keys into .env
9.Create new schemas on confluent cloud
10.Generate new API Keys for schemas on confluent cloud
11.Copy schemas registry url + API Keys into .env
12.Create new cluster on mongoDB atlas
13.Create database access on mongoDB atlas
14.Set up IP access list on mongoDB atlas
15.Generate new API Keys for cluster
16.Copy mongoDB url + API Keys into .env
17.Run this command on CLI
```
$ conda create -n <environment_name> python=<insert_version>
$ conda env list
$ conda activate <environment_name>
$ pip install -r requirements.txt
```
18.Run producer.py to send data into consumer
19.Run consumer.py to recieve data and save the data into mongoDB
20.Run ML_Recruitment_Prediction.py and save the data into postgreSQL
21.Stop and remove the docker container (after finish extracting)
21a.Stop and remove the container
```
docker compose docker-compose.yaml down --remove-orphans -v
```

## 6. Kafka producer
In this case, kafka producer start serialize using 'SerializingProducer' from confluent_kafka module convert tabular data to Avro type before sending to consumer.
To make sure all messages have been send to consumer use 'producer.flush()'.
![image](https://github.com/vnobets7/final_project_ftde_ricky/blob/ftde-dev/Data-ingestion/data-stream-processing/images/SS-final-project-1.PNG)
<br>
On the confluent cloud, i'm already create a kafka topic credentials and kafka schema registry credentials before running the producer.py
<br>
note: for this project, i'm using confluent cloud that connect to the kafka cluster. Kafka producer also create schema name 'candidate' on confluent cloud.
![kafka-schema](https://github.com/vnobets7/final_project_ftde_ricky/blob/ftde-dev/Data-ingestion/data-stream-processing/images/SS-final-project-3-1.PNG)

## 7. Kafka consumer
In this case, kafka consumer receive all transaction data from producer on kafka cluster and save the data into mongoDB on noSQL mongoDB cluster.
Before saving into mongoDB, kafka consumer start deserialize using 'DeserializingConsumer' from confluent_kafka module convert Avro to JSON NoSQL datatype.
![image](https://github.com/vnobets7/final_project_ftde_ricky/blob/ftde-dev/Data-ingestion/data-stream-processing/images/SS-final-project-2-1.PNG)
<br>
note: for this project, i'm using mongoDB atlas that connect from local using docker-compose and pymongo.

## 8. Machine learning 
(under maintenance)

## Extract data from mongoDB to PostgreSQL
In this case, data inside mongoDB will be convert from JSON datatype to tabular data and save into postgreSQL.
Before save into postgreSQL, schema table already been create using raw sql inside python code.
For this project, i'm using dbeaver and docker compose to access postgreSQL connection.
![mongo-to-postgres](https://github.com/vnobets7/final_project_ftde_ricky/blob/ftde-dev/Data-ingestion/data-stream-processing/images/SS-final-project-5.PNG)

## 9. Database result
- MongoDB atlas cluster
![mongo-cluster](https://github.com/vnobets7/final_project_ftde_ricky/blob/ftde-dev/Data-ingestion/data-stream-processing/images/SS-final-project-4.PNG)
<br>
- Staging-db/stream_processing_ricky: data_training_development
