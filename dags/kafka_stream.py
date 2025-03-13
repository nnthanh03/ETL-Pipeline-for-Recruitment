import uuid
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2023, 9, 3, 10, 00)
}

def get_data():
    import requests

    res = requests.get("https://api-new-jobs.vercel.app/items")
    res = res.json()
    res = res[0]

    return res



def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging

    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60: #1 minute
            break
        try:
            res = get_data()

            producer.send('jobs_created', json.dumps(res).encode('utf-8'))
        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue


with DAG('jobs_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )


# def stream_data():
#     import json
#     from kafka import KafkaProducer
#     import time

#     res = get_data()

#     producer = KafkaProducer(bootstrap_servers=['localhost:9092'], max_block_ms=5000)

#     producer.send('jobs_created', json.dumps(res).encode('utf-8'))

# stream_data()

# docker exec -it realtime-data-streaming-webserver-1 airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password admin
