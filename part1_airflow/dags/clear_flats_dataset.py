from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
from steps.clear_flats_data import create_table, extract, transform, load
from steps.messages import send_telegram_success_message, send_telegram_failure_message


with DAG(
    dag_id='clear_flats_dataset',
    schedule='@once',
    catchup=False,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    tags=["ETL"],
    on_success_callback=send_telegram_success_message,
    on_failure_callback=send_telegram_failure_message
) as dag:

    create_table = PythonOperator(task_id='create_table', python_callable=create_table)
    extract = PythonOperator(task_id='extract', python_callable=extract)
    transform = PythonOperator(task_id='transform', python_callable=transform)
    load = PythonOperator(task_id='load', python_callable=load)

    create_table >> extract >> transform >> load