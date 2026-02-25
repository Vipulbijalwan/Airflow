from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from airflow.timetables.interval import CronDataIntervalTimetable
import requests
import pendulum
import json


dag = DAG(
    dag_id="incremental_api_data_load",
    start_date=datetime(2026, 2, 25),
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone=pendulum.timezone("Asia/Kolkata")),
)
def fetch_api_data(url,output_path,**context):
    start_date = context['ds']
    end_date = context['ds']
    payload = json.dumps({
        "start_date": start_date,
        "end_date": end_date})
    
    headers = {  
            'accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': 'Basic YWRtaW46bWFuaXNo'
}

    response = requests.request("POST", url, headers=headers, data=payload)

    print(response.text)
    data= response.json()
   
    with open(output_path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"Data saved to {output_path}")

pull_api_data = PythonOperator(
    dag=dag,
    task_id="pull_api_data",
    python_callable=fetch_api_data,
    op_args=["http://fastapi-app:5000/getAll", 
             "/opt/airflow/output_files/api_data.json"]
)