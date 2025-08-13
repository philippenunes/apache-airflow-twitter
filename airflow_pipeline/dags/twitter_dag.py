import sys
sys.path.append("airflow_pipeline")

from airflow.models import DAG
from datetime import datetime, timedelta
from operators.twitter_operator import TwitterOperator
from os.path import join


with DAG(dag_id="TwitterDAG", start_date=datetime.now() - timedelta(days=2), schedule="@daily") as dag:
    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"
    query = "data science"
    to = TwitterOperator(file_path=join("datalake/twitter_datascience",
                                        "extract_date={{ ds }}",
                                        "datascience_{{ ds_nodash }}.json"),
                                        query=query,
                                        start_time="{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
                                        end_time="{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
                                        task_id="twitter_datascience")