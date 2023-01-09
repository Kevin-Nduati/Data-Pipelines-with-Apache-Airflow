import airflow.utils.dates as dates
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.python import PythonSensor

from pathlib import Path



dag = DAG(
    dag_id = "python_sensor",
    start_date=dates.days_ago(3),
    schedule_interval="0 16 * * *",
    description="A batch workflow for ingesting supermarket promotions data, demonstrating the FileSensor",
    default_args={"depends_on_past": True}
)

create_metrics = EmptyOperator(
    task_id = "create_metrics",
    dag = dag
)

def wait_for_supermarket(supermarket_id):
    supermarket_path = Path("/data/" + supermarket_id)
    data_files = supermarket_path.glob("data-*.csv")
    success_file = supermarket_path / "_SUCCESS"
    return data_files and success_file.exists()

for supermarket_id in [1,2,3,4]:
    wait = PythonSensor(
        task_id = f"wait_for_supermarket_{supermarket_id}",
        python_callable=wait_for_supermarket,
        op_kwargs={"supermarket_id": f"supermarket{supermarket_id}" },
        timeout = 600,
        dag = dag
    )
    copy = EmptyOperator(task_id = f"Copy_to_raw_supermarket_{supermarket_id}", dag=dag)
    process = EmptyOperator(task_id = f"process_to_raw_supermarket_{supermarket_id}", dag=dag)
    wait >> copy >> process >> create_metrics