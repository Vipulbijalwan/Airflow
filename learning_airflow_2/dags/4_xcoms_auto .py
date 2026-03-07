from airflow.sdk import dag, task
from datetime import datetime


@dag(
    dag_id="xcoms_dag_auto",
    
)
def xcoms_dag_auto():

    @task
    def first_task():
        print("Extracting data....")
        fetched_data = {"data": [1, 2, 3, 4, 5, 6]}
        return fetched_data

    @task
    def second_task(data: dict):
        fetched_data = data['data']
        transformed_data = [x * 2 for x in fetched_data]
        transformed_data_dict = {"transf_data": transformed_data}
        return transformed_data_dict

    @task
    def third_task(data: dict):
        print("Loading data:", data)
        return data

    first = first_task()
    second = second_task(first)
    third = third_task(second)


dag = xcoms_dag_auto()