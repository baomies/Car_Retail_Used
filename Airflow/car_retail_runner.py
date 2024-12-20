from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

# Default arguments
default_args = {
    'owner': 'Azzam',
    'depends_on_past': False,
    'email': ["abdrhmnza67@gmail.com"],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG
dag = DAG(
    'car_retail_runner',
    default_args=default_args,
    description='Run PySpark Script for Car Retail Data Processing',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 11, 15),
    catchup=False
)


# Task untuk menjalankan Spark script bagian cleaning
car_retail_clean = BashOperator(
    task_id="clean_runner",
    bash_command=(
        "spark-submit "
        "--jars /home/hadoop/postgresql-42.2.26.jar "
        "/home/hadoop/airflow/scripts/car_retail_clean.py"
    ),
    dag=dag
)

# Task untuk menjalankan Spark script bagian case
car_retail_case = BashOperator(
    task_id="case_runner",
    bash_command=(
        "spark-submit "
        "--jars /home/hadoop/postgresql-42.2.26.jar "
        "/home/hadoop/airflow/scripts/car_retail_case.py"
    ),
    dag=dag
)

# Task untuk menjalankan Spark script
car_retail_validation = BashOperator(
    task_id="validation_runner",
    bash_command=(
        "spark-submit "
        "--jars /home/hadoop/postgresql-42.2.26.jar "
        "/home/hadoop/airflow/scripts/car_retail_validation.py"
    ),
    dag=dag
)

car_retail_clean >> car_retail_case >> car_retail_validation
