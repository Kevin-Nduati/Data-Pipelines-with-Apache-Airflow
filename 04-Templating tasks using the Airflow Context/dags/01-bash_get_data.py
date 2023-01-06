import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
import airflow.utils.dates as dates


dag = DAG(
    dag_id = "listing_4",
    start_date= dates.days_ago(3),
    schedule_interval= "@hourly"
)

get_data = BashOperator(
    task_id = "bash_get_data",
    bash_command=(
        "curl -o /tmp/wikipageviews.gz "
        "https://dumps.wikimedia.org/other/pageviews/"
        "{{ execution_date.year }}/"
        "{{ execution_date.year}} - {{ '{:02}'.format(execution_date.month) }}/"
        "pageviews - {{ execution_date.year }}"
        "{{ '{:02}'.format(execution_date.month) }}"
        "{{ '{:02}'.format(execution_date.day) }}-"
        "{{ '{:02}'.format(execution_date.hour) }}0000.gz"
    ),
    dag = dag
)