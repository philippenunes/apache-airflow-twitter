import sys
sys.path.append("airflow_pipeline")

from airflow.models import BaseOperator, DAG, TaskInstance
from hook.twitter_hook import TwitterHook
from datetime import datetime, timedelta
import json

class TwitterOperator(BaseOperator):

    def __init__(self, end_time, start_time, query, **kwargs):
        self.end_time = end_time
        self.start_time = start_time
        self.query = query
        super().__init__(**kwargs)

    def execute(self, context):
        with open("extract_twitter.json", "w") as output_file:
          for page in TwitterHook(self.end_time, self.start_time, self.query).run():
            json.dump(page, output_file, ensure_ascii=False)   
            output_file.write("\n")

if __name__ == "__main__":
    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

    end_time = datetime.now().strftime(TIMESTAMP_FORMAT)
    start_time = (datetime.now() + timedelta(-1)).date().strftime(TIMESTAMP_FORMAT)
    query = "data science"

    with DAG(dag_id="TwitterTest", start_date=datetime.now()) as dag:
      to = TwitterOperator(query=query, start_time=start_time, end_time=end_time, task_id="test_run")
      ti = TaskInstance(task=to)
      to.execute(ti.task_id)