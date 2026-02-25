from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

dag = DAG(
    dag_id="myfirst_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
)

copy_file = BashOperator(
    task_id="copy_file",
    bash_command="echo 'Copying file...'",
    dag=dag
)

def print_context(**kwargs):
    print("This is task 2")
    print("job completed")

task2 = PythonOperator(
    task_id="task2",
    python_callable=print_context,
    dag=dag
)

copy_file >> task2