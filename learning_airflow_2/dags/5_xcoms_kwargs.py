from airflow.sdk import dag, task
from datetime import datetime


@dag(
    dag_id="xcoms_dag_manual",
    
)
def xcoms_dag_manual():

    @task
    def first_task(**kwargs):

        # Extracting "ti" from kwargs

        ti = kwargs['ti']
        print("Extracting data....")
        fetched_data = {"data": [1, 2, 3, 4, 5, 6]}
        ti.xcom_push(key="return_result",value=fetched_data)
        return fetched_data

    @task
    def second_task(**kwargs):

        ti=kwargs['ti']

        fetched_data = ti.xcom_pull(task_ids="first_task",key='return_result')['data']
        transformed_data = [x * 2 for x in fetched_data]
        transformed_data_dict = {"transf_data": transformed_data}
        
        ti.xcom_push(key="transformed_data_dict",value=transformed_data_dict)
        return transformed_data_dict

    @task
    def third_task(**kwargs):
        ti=kwargs['ti']
        ti.xcom_push(key)
        return data

    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third


dag = xcoms_dag_manual()