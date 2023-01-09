import airflow.utils.dates as dates
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor

"""
Sensors continuously poll for certain conditions to be true and succeed if so. If false, the sensor
will wait and try again until either the condition is true or a timeout is eventually reached
"""

dag = DAG(
    dag_id = "triggers_01",
    start_date=dates.days_ago(3),
    schedule_interval="0 16 * * *",
    description="A batch workflow for ingesting supermarket promotions data, demonstrating the FileSensor",
    default_args={"depends_on_past": True}
)

create_metrics = EmptyOperator(
    task_id = "create_metrics",
    dag = dag
)

for supermarket_id in [1,2,3,4]:
    wait = FileSensor(
        task_id = f"wait_for_supermarket_{supermarket_id}",
        filepath = f"/data/supermarket{supermarket_id}/data.csv"
    )
    copy = EmptyOperator(task_id = f"Copy_to_raw_supermarket_{supermarket_id}", dag=dag)
    process = EmptyOperator(task_id = f"process_to_raw_supermarket_{supermarket_id}", dag=dag)
    wait >> copy >> process >> create_metrics