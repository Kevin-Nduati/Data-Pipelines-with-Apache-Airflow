import airflow
from airflow import DAG
from airflow.operators.empty import EmptyOperator
import airflow.utils.dates as dates


with DAG(
    dag_id="dummy_dag",
    start_date= dates.days_ago(3),
    schedule="@daily"
) as dag:
    start = EmptyOperator(task_id = "start")

    fetch_sales = EmptyOperator(task_id='fetch_sales')
    clean_sales = EmptyOperator(task_id='clean_sales')

    fetch_weather = EmptyOperator(task_id='fetch_weather')
    clean_weather = EmptyOperator(task_id='clean_weather')

    join_datasets = EmptyOperator(task_id='join_datasets')
    train_model = EmptyOperator(task_id='train_model')
    deploy_model = EmptyOperator(task_id='deploy_model')


    start >> [fetch_sales, fetch_weather]
    fetch_sales >> clean_sales
    fetch_weather >> clean_weather
    [clean_sales, clean_weather] >> join_datasets # this is called fan-in structure
    join_datasets >> train_model >> deploy_model