from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator
import scrape_data_from_bikroy

default_arg={
    'owner':'morshed',
    'start_date':datetime(2024,1,1),
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':2,
    'retry_delay':timedelta(minutes=5)
}

dag=DAG(
    'scrape_job_data',
    default_args=default_arg,
    description= 'A DAG to scrape job posting data from bikroy.com',
    schedule_interval="0 0 1 * *",
    priority_weight=1
)

job_task = PythonOperator(
    task_id='job_task_id',
    dag=dag,
    python_callable=scrape_data_from_bikroy
)

job_task

