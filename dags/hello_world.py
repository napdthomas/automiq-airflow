# dags/hello_world.py
from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta

default_args = {
    "owner": "automiq",
    "start_date": datetime(2025, 1, 1),
    "retries": 0,
}

with DAG(
    "hello_world",
    default_args=default_args,
    schedule=None,
    catchup=False,
    description="Simple hello world DAG to test git-sync",
    params={
        "name": "World",
        "message": "Hello from Airflow!"
    },
) as dag:

    @task
    def say_hello(**context):
        params = context["params"]
        name = params.get("name", "World")
        message = params.get("message", "Hello")
        
        print(f"{message}, {name}!")
        print(f"This DAG was synced from git at {datetime.now()}")
        return f"{message}, {name}!"

    @task
    def say_goodbye(greeting_result):
        print(f"Previous task said: {greeting_result}")
        print("Goodbye! DAG completed successfully.")
        return "Done"

    hello_result = say_hello()
    say_goodbye(hello_result)
