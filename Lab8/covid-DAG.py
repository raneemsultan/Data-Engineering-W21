# step 1 - import modules
import requests
import json

from airflow import DAG
from datetime import datetime
from datetime import date
# Operators; we need this to operate!
from airflow.operators.python_operator import PythonOperator

import pandas as pd

# step 2 - define default args
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 12, 13)
    }


# step 3 - instantiate DAG
dag = DAG(
    'covid-DAG',
    default_args=default_args,
    description='Fetch covid data from API',
    schedule_interval='@once',
)

# step 4 Define tasks
def store_data(**context):
    df = context['task_instance'].xcom_pull(task_ids='extract_data')
    df = df.set_index("date_of_interest")
    df.to_csv("data/nyccovid.csv")


def extract_data(**kwargs):
    url = "https://data.cityofnewyork.us/resource/rc75-m7u3.json"
    response = requests.get(url)
    df = pd.DataFrame(json.loads(response.content))
    return df



t1 = PythonOperator(
    task_id='extract_data',
    provide_context=True,
    python_callable=extract_data,
    dag=dag,
)

t2 = PythonOperator(
    task_id='store_data',
    provide_context=True,
    python_callable=store_data,
    dag=dag,
)

# step 5 - define dependencies
t1 >> t2
