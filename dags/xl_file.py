from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
from datetime import timedelta
import datetime as dt

default_args = {
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def preprocess_excel_file():
    # Read Excel file
    data = pd.read_excel('/opt/airflow/data/OnlineRetail.xlsx')
    # Write processed data to CSV file
    data.to_csv('/opt/airflow/data/data.csv', index=False)

with DAG('excel_preprocessing', default_args=default_args, schedule_interval=None) as dag:
    preprocess_task = PythonOperator(
        task_id='preprocess_task',
        python_callable=preprocess_excel_file
    )
