from airflow.sdk import dag, task
from datetime import datetime

@dag(
    dag_id="versioned_dag"
)
def versioned_dag():

    @task.python
    def first_task():
        print("This is the first task")

    @task.python
    def second_task():
        print("This is the second task")

    @task.python
    def third_task():
        print("This is the third task")

    @task.python
    def version_task():
        print("This is the third task : version 2.0")

    first_task() >> second_task() >> third_task()>>version_task()

dag = versioned_dag()