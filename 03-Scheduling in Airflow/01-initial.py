from datetime import datetime
from pathlib import Path
import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id = 'fetch_events',
    start_date=datetime(2019,1,1),
    schedule_interval=None # specify that this is an unspecified dag
)


fetch_events = BashOperator(
    task_id = "fetch_events",
    bash_command = (
        "mkdir -p /data/events && "
        "curl -o /data/events.json http://events_api:5000/events"
    ),
    dag = dag
)


def calculate_stats(input_path, output_path):
    """
    Calculate event statistics
    """

    events = pd.read_json(input_path)
    # load the events and calculate the required statistics
    stats = events.groupby(['date', 'user']).size().reset_index()
    # ensure the output directory exists
    Path(output_path).parent.mkdir(exist_ok=True)

    stats.to_csv(output_path, index=False)


calculate_stats = PythonOperator(
    task_id = 'calculate_stats',
    python_callable= calculate_stats,
    op_kwargs={"input_path": "/data/events.json", "output_path": "/data/stats.csv"},
    dag = dag
)

fetch_events >> calculate_stats