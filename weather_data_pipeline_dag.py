from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Define default arguments for the DAG
default_args = {
    'owner': 'jesus-guijarro',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
# Define the folder path where the scripts are located
folder_path = '/home/jfgs/Projects/weather-spain'

# Create the DAG
with DAG(
    'weather_data_pipeline',
    default_args=default_args,
    description='A DAG to run weather data scripts',
    schedule_interval='10 20 * * *',  # This sets the DAG to run daily at 20:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # Task to run weather_data.py at 20:10
    run_weather_data = BashOperator(
        task_id='run_weather_data',
        bash_command=f'cd {folder_path} && python weather_data.py',
        execution_timeout=timedelta(minutes=5),  # This task has a maximum execution time of 10 minutes
        dag=dag,
    )

    # Task to run weather_etl_job.py at 20:15
    run_weather_etl = BashOperator(
        task_id='run_weather_etl',
        bash_command=f'cd {folder_path} && python weather_etl_job.py',
        dag=dag,
    )

    # Set task dependencies
    run_weather_data >> run_weather_etl
