import airflow
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
import airflow.utils.dates as dates



ERP_CHANGE_DATE = dates.days_ago(1)

def pick_erp_system(**context):
    if context["execution_date"] < ERP_CHANGE_DATE:
        return "fetch_sales_old"
    else:
        return "fetch_sales_new"

def fetch_sales_old(**context):
    print("Fetching sales data (OLD)...")


def fetch_sales_new(**context):
    print("Fetching sales data (NEW)...")


def clean_sales_old(**context):
    print("Preprocessing sales data (OLD)...")


def clean_sales_new(**context):
    print("Preprocessing sales data (NEW)...")    


with DAG(
    dag_id = "branching_erp",
    start_date= dates.days_ago(3),
    schedule_interval="@daily"
) as dag:
    start = EmptyOperator(task_id = "start")

    pick_erp_system = BranchPythonOperator(task_id = 'pick_erp_system', python_callable=pick_erp_system)

    fetch_sales_old = PythonOperator(task_id = "fetch_sales_old", python_callable=fetch_sales_old)
    clean_sales_old = PythonOperator(task_id = "clean_sales_old", python_callable=clean_sales_old)

    fetch_sales_new = PythonOperator(task_id = "fetch_sales_new", python_callable=fetch_sales_new)
    clean_sales_new = PythonOperator(task_id = "clean_sales_new", python_callable=clean_sales_new)

    fetch_weather = EmptyOperator(task_id = "fetch_weather")
    clean_weather = EmptyOperator(task_id = "clean_weather")

    join_datasets = EmptyOperator(task_id = "join_datasets", trigger_rule="none_failed")
    train_model = EmptyOperator(task_id = "train_model")
    deploy_model = EmptyOperator(task_id = "deploy_model")


    start >> [pick_erp_system, fetch_weather]
    pick_erp_system >> [fetch_sales_old, fetch_sales_new]
    fetch_sales_old >> clean_sales_old
    fetch_sales_new >> clean_sales_new
    fetch_weather >> clean_weather
    [clean_sales_old, clean_sales_new, clean_weather] >> join_datasets
    join_datasets >> train_model >> deploy_model