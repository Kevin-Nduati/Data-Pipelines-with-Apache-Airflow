from urllib import request
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import airflow.utils.dates as dates

dag = DAG(
    dag_id = 'stockSense',
    start_date= dates.days_ago(1),
    schedule_interval="@daily"
)


def _get_data(year, month, day, hour, output_path, **_):
    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
    )
    request.urlretrieve(url, output_path)


get_data = PythonOperator(
    task_id = "get_data",
    python_callable=_get_data,
    op_kwargs={
        'year': "{{ execution_date.year }}",
        'month': "{{ execution_date.month }}",
        'day': "{{ execution_date.day }}",
        'hour': "{{ execution_date.hour }}",
        'output_path': "/tmp/wikipageviews.gz"
    },
    dag = dag
)

extract_gz = BashOperator(
    task_id = 'extract_gz',
    bash_command= "gunzip --force /tmp/wikipageviews.gz",
    dag = dag
)

def fetch_pageviews(pagenames, execution_date):
    result = dict.fromkeys(pagenames, 0)
    # open the file written in previous task
    with open("/tmp/wikipageviews", 'r') as f:
        for line in f:
            domain_code, page_title, view_counts, _ = line.split(" ")
            # filter for en only and check if page_titles is in given pagenames
            if domain_code == 'en' and page_title in pagenames:
                result[page_title] = view_counts

    with open('tmp/postgres_query.sql', 'w') as f:
        for pagename, pageviewcount in result.items():
            f.write(
                "INSERT INTO pageview_counts VALUES ("
                f"'{pagename}', {pageviewcount}, '{execution_date}'"
                ");\n"
            )


fetch_pageviews = PythonOperator(
    task_id = 'fetch_pageviews',
    python_callable= fetch_pageviews,
    op_kwargs={
        "pagenames": {"Google", "Amazon", "Apple", "Microsoft", "Facebook"}
    },
    dag = dag
)

write_to_postgres = PostgresOperator(
    task_id = "write_to_postgres",
    postgres_conn_id = "my_postgres",
    sql = """
    CREATE TABLE pageview_counts (
    pagename VARCHAR(50) NOT NULL,
    pageviewcount INT NOT NULL,
    datetime TIMESTAMP NOT NULL
);
""",
    dag = dag
)

get_data >> extract_gz >> fetch_pageviews >> write_to_postgres
