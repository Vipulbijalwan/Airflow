from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator
from datetime import datetime

@dag(
    dag_id="operator_dag"
    
)
def bash_dag():

    @task
    def first_task():
        print("This is the first task")

    @task
    def second_task():
        print("This is the second task")

    @task.bash
    def bash_task_morden():
        return "echo https://airflow.apache.org/"

    bash_task_old = BashOperator(
        task_id="bash_task_old",
        bash_command="echo https://airflow.apache.org/",
    )

    first = first_task()
    second = second_task()
    modern = bash_task_morden()

    first >> second >> modern >> bash_task_old


dag = bash_dag()