# We'll start by importing the DAG object
from datetime import timedelta

from airflow import DAG
# We need to import the operators used in our tasks
from airflow.operators.python_operator import PythonOperator
# We then import the days_ago function
from airflow.utils.dates import days_ago

import pandas as pd
import sqlite3
import os

# get dag directory path
dag_path = os.getcwd()


def transform_data():
    booking = pd.read_csv(f"{dag_path}/raw_data/dataset.csv", low_memory=False)


    booking.to_csv(f"{dag_path}/processed_data/processed_data.csv", index=False)


def load_data():
    conn = sqlite3.connect("/usr/local/airflow/db/datascience.db")
    c = conn.cursor()
    c.execute('''
                CREATE TABLE IF NOT EXISTS booking_record (
                    City   TEXT NOT NULL,
                    Region TEXT NOT NULL,
                    Country TEXT(512) NOT NULL,
                    AirQuality   TEXT NOT NULL,
                    WaterPollution TEXT
                );
             ''')
    records = pd.read_csv(f"{dag_path}/processed_data/processed_data.csv")
    records.to_sql('booking_record', conn, if_exists='replace', index=False)


# initializing the default arguments that we'll pass to our DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(5)
}

ingestion_dag = DAG(
    'booking_ingestion',
    default_args=default_args,
    description='Aggregates booking records for data analysis',
    schedule_interval=timedelta(days=1),
    catchup=False
)

task_1 = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=ingestion_dag,
)

task_2 = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=ingestion_dag,
)


task_1 >> task_2
