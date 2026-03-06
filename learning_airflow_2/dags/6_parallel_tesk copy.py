from airflow.decorators import dag, task
from datetime import datetime


@dag(
    dag_id="parallel_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
)
def parallel_dag():

    @task
    def extract_task(**kwargs):

        print("EXT DATA.......")

        ti = kwargs['ti']

        extracted_data_dict = {
            "api_extracted_data": [1, 2, 3, 4],
            "db_extracted_data": [4, 5, 6, 7],
            "s3_extracted_data": [7, 8]
        }

        ti.xcom_push(
            key='return_result',
            value=extracted_data_dict
        )


    @task
    def transform_task_api(**kwargs):

        ti = kwargs['ti']

        api_extracted_data = ti.xcom_pull(
            task_ids="extract_task"
        )["api_extracted_data"]

        print("Transforming API data....")

        transformed_api_data = [
            x * 100 for x in extracted_data_dict['api_extracted_data']
        ]

        ti.xcom_push(
            key="transformed_api_data",
            value=transformed_api_data
        )


    @task
    def transform_task_db(**kwargs):

        ti = kwargs['ti']

        extracted_data_dict = ti.xcom_pull(
            task_ids="extract_task",
            key="extracted_data_dict"
        )

        print("Transforming DB data....")

        transformed_db_data = [
            x * 100 for x in extracted_data_dict['db_extracted_data']
        ]

        ti.xcom_push(
            key="transformed_db_data",
            value=transformed_db_data
        )


    @task
    def transform_task_s3(**kwargs):

        ti = kwargs['ti']

        extracted_data_dict = ti.xcom_pull(
            task_ids="extract_task",
            key="extracted_data_dict"
        )

        print("Transforming S3 data....")

        transformed_s3_data = [
            x * 100 for x in extracted_data_dict['s3_extracted_data']
        ]

        ti.xcom_push(
            key="transformed_s3_data",
            value=transformed_s3_data
        )


    @task
    def load_task(**kwargs):

        ti = kwargs['ti']

        api_data = ti.xcom_pull(
            task_ids="transform_task_api",
            key="transformed_api_data"
        )

        db_data = ti.xcom_pull(
            task_ids="transform_task_db",
            key="transformed_db_data"
        )

        s3_data = ti.xcom_pull(
            task_ids="transform_task_s3",
            key="transformed_s3_data"
        )

        print("Loading data....")

        print(api_data)
        print(db_data)
        print(s3_data)

    extract = extract_task()
    transform_api = transform_task_api()
    transform_db = transform_task_db()
    transform_s3 = transform_task_s3()
    load = load_task()

    extract >> [transform_api, transform_db, transform_s3] >> load


parallel_dag()