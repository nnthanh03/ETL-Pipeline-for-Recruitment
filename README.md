# ETL-Pipeline-for-Recruitment

## Introduction
- This project uses Apache Spark (with PySpark) to Extract-Transform-Load (ETL) data from a Data Lake (CassandraDB) into a Data Warehouse (MySQL database) with Star-schema for a Recruitment system.
- Using Batch Processing technique to automate the ETL process (Near Real-time).
- The project also uses Docker to install and run the services.
- Using Kafka to Streaming data from my fake source.
- Using Airflow to schedule data ingestion and transfer to Cassandra.

## Requirements
- Docker
- Docker Compose

## Installation
- Clone the project from the Github repository:
```bash
git clone https://github.com/nnthanh03/ETL-Pipeline-for-Recruitment.git
```
- Start the Docker containers using Docker Compose:
```bash
docker-compose up -d
```
- Start streaming data to CassandraDB:
```bash
python py_stream_db.py
```
- Start ETL with batch processing:
```bash
python ETL_Pipeline_With_CDC.py
```

## Result

- Raw data:
  ![image](https://github.com/user-attachments/assets/36d0473c-3625-4ebc-8aae-d6cd6d960159)

  + Schema:
    
    <img src="https://github.com/user-attachments/assets/68cbfbc0-e17c-42d6-87ef-f0772c73a356" width="35%">


- Final data:
![image](https://github.com/user-attachments/assets/a3462776-79c1-44b6-94e7-9260bb631535)

  + Schema:
    
    <img src="https://github.com/user-attachments/assets/b1ca6f6b-a686-4543-b4b9-6a8a672ca2a7" width="45%">



