from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator
from scrape_data_from_scrapethissite import scrape_countries_data,scrape_hockey_teams_data
default_arg={
    'owner':'morshed',
    'start_date':datetime(2024,1,1),
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':2,
    'retry_delay':timedelta(minutes=5)
}

dag=DAG(
    'scrape_country_hockey_data',
    default_args=default_arg,
    description= 'A DAG to scrape country and hockey teams data from scrapethissite.com',
    schedule_interval="0 0 * * 0",
    priority_weight=2
)

scrape_country_task = PythonOperator(
    task_id = 'scrape_country',
    dag=dag,
    python_callable=scrape_countries_data,
)
scrape_hockey_teams_task = PythonOperator(
    task_id = 'scrape_hockey_teams',
    dag=dag,
    python_callable=scrape_hockey_teams_data,
)

scrape_country_task >> scrape_hockey_teams_task
