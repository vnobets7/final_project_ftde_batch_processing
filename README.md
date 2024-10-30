# Final Project End-to-end Fast-track Data Engineering Digital Skola

### Step to run the project
1. Navigate to the data batch processing project and run the DAG "dag1 data-ingestion"
2. Navigate to the data stream processing project and run the following code in order: producer.py -> kafka_consumer_to_mongoDB.py -> ML_Recruitment_Prediction.py
3. Navigate to the data warehouse design project and run the DAG "dag2-data-ingestion-dwh"
4. Navigate to the data transformation with spark and run the DAG "dag3-transform-with-spark"
5. Navigate to the data visualization with looker studio 
```
access the google looker studio in this link: https://lookerstudio.google.com/reporting/6f4df4a8-29bb-4588-9819-0db34fff96f4
```

### Architecture
![Local-Database](https://github.com/vnobets7/final_project_ftde_ricky/blob/main/images/arch-pic.png)

### Futher improvement:
- Use python venv instead conda
- Add github workflows into the project repo
- Re-create the data streaming project and adding apache code for orchestrate the pipeline
- Use python or other tools for automate data quality(DQ) check
- Create dashboard for employee view with different department
- Create dashboard for candidate
- Use google cloud service to move the project that run on-premise to the cloud environment
