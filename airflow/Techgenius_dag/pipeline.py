from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from utils import extract_data, transform_data, load_data2

default_args = {
    "owner" : "airflow",
    "email" : "nelxzxi@gmail.com",
    "email_on_failure" : False,
    "depends_on_past" : False,
    "retries" : 1,
    "retry_delay" : timedelta(minutes=1)
}


with DAG(
    'Techgenius_data_pipeline',
    default_args=default_args,
    description='Techgenius data pipeline',
    schedule_interval=timedelta(weeks=2),
    start_date=days_ago(1),
    tags=['ETL', 'Techgenius data pipeline', 'Data Engineering']
) as dag:

    
    extract_reviews_data = PythonOperator(
        task_id = 'extract_reviews_data',
        python_callable = extract_data
    )


    transform_reviews_data = PythonOperator(
        task_id = 'transform_reviews_data',
        python_callable = transform_data
    )


    load_reviews_data2 = PythonOperator(
        task_id = 'load_reviews_data2',
        python_callable = load_data2
    )



    extract_reviews_data >> transform_reviews_data >> load_reviews_data2
