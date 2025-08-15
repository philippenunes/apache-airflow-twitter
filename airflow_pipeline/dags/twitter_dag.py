import sys
sys.path.append("airflow_pipeline")

from airflow.models import DAG
from airflow.timetables.interval import CronDataIntervalTimetable
from datetime import datetime
from os.path import join
from operators.twitter_operator import TwitterOperator
from pendulum import datetime, timezone


with DAG(
    dag_id="TwitterDAG",
    start_date=datetime(2025, 8, 14, tz="America/Sao_Paulo"),
    schedule=CronDataIntervalTimetable("@daily", timezone("America/Sao_Paulo")),
    catchup=True
) as dag:

    to = TwitterOperator(
        file_path=join(
            "datalake/twitter_datascience",
            "extract_date={{ ds }}",
            "datascience_{{ ds_nodash }}.json"
        ),
        query="data science",
        # conn_id="twitter-api",
        start_time="{{ data_interval_start | ds }}T00:00:00Z",
        end_time="{{ data_interval_end | ds }}T00:00:00Z",
        task_id="twitter_datascience"
    )