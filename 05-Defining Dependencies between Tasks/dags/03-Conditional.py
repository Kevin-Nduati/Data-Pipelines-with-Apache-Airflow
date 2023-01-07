import airflow
import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
import airflow.utils.dates as dates
from airflow.exceptions import AirflowSkipException



ERP_CHANGE_DATE = dates.days_ago(1)

def pick_erp_system(**context):
    if context["execution_date"] < ERP_CHANGE_DATE:
        return "fetch_sales_old"
    else:
        return "fetch_sales_new"




def latest_only(**context):
    now = pendulum.now("UTC")
    left_window = context["dag"].following_schedule(context["execution_date"])
    right_window = context["dag"].following_schedule(left_window)

    if not left_window < now <= right_window:
        raise AirflowSkipException()

def fetch_sales_old(**context):
    print("Fetching sales data (OLD)...")


def fetch_sales_new(**context):
    print("Fetching sales data (NEW)...")


def clean_sales_old(**context):
    print("Preprocessing sales data (OLD)...")


def clean_sales_new(**context):
    print("Preprocessing sales data (NEW)...")    


with DAG(
    dag_id = "condition_function",
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
    latest_only = PythonOperator(task_id="latest_only",  python_callable = latest_only)
    deploy_model = EmptyOperator(task_id = "deploy_model")


    start >> [pick_erp_system, fetch_weather]
    pick_erp_system >> [fetch_sales_old, fetch_sales_new]
    fetch_sales_old >> clean_sales_old
    fetch_sales_new >> clean_sales_new
    fetch_weather >> clean_weather
    [clean_sales_old, clean_sales_new, clean_weather] >> join_datasets
    join_datasets >> train_model >> deploy_model
    latest_only >> deploy_model
