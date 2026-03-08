from airflow.sdk import dag,task
from pendulum import datetime
from airflow.timetables.interval import CronDataIntervalTimetable


@dag(
    dag_id="first_incremental_load_dag",
    start_date=datetime(2026, 3, 4, tz="Asia/Kolkata"),
    schedule=CronDataIntervalTimetable("@daily", timezone="Asia/Kolkata"),
    catchup=True,
    is_paused_upon_creation=False
)
def first_incremental_load_dag():

    @task.python
    def incremental_load(**kwargs):
        date_interval_start = kwargs["data_interval_start"]
        date_interval_end = kwargs["data_interval_end"]
        

        print(f"Data Interval Start: {date_interval_start}")
        print(f"Data Interval End: {date_interval_end}")

    @task.bash
    def incremental_load_bash():
        return "echo 'Executing incremental load bash script' {{data_interval_start}} to {{data_interval_end}}"

    incremental_load() >> incremental_load_bash()

first_incremental_load_dag()
