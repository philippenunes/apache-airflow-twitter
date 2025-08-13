import sys
sys.path.append("airflow_pipeline")

from airflow.models import DAG
from datetime import datetime, timedelta
from operators.twitter_operator import TwitterOperator
from os.path import join


with DAG(dag_id="TwitterTest", start_date=datetime.now()) as dag:
    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"
    end_time = datetime.now().strftime(TIMESTAMP_FORMAT)
    start_time = (datetime.now() + timedelta(-1)).date().strftime(TIMESTAMP_FORMAT)
    query = "data science"
    to = TwitterOperator(file_path=join("datalake/twitter_datascience",
                                        "extract_date={{ ds }}",
                                        "datascience_{{ ds_nodash }}.json"),
                                        query=query, start_time=start_time, end_time=end_time, task_id="test_run")