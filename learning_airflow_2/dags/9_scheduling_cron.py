from airflow.sdk import dag, task
from pendulum import datetime
from airflow.timetables.trigger import CronTriggerTimetable

@dag(
    dag_id="first_scheduled_dag",
    start_date=datetime(2026, 3, 4, tz="Asia/Kolkata"),
    schedule=CronTriggerTimetable("0 16 * * *", timezone="Asia/Kolkata"),
    catchup=False,
    is_paused_upon_creation=False
)
def first_scheduled_dag():

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

first_scheduled_dag()