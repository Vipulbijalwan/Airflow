from airflow.sdk import dag, task
from datetime import datetime

@dag(
    dag_id="first_dag"
)
def first_dag():

    @task
    def first_task():
        print("This is the first task")

    @task
    def second_task():
        print("This is the second task")

    @task
    def third_task():
        print("This is the third task")

    first_task() >> second_task() >> third_task()

dag = first_dag()