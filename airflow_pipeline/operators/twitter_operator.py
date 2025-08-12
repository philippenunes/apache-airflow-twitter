import airflow from BaseOperator
from hook.twitter_hook import TwitterHook
import json

class TwitterOperator(BaseOperator):

    def __init__(self, end_time, start_time, query, **kwargs):
        self.end_time = end_time
        self.start_time = start_time
        self.query = query
        super().__init__(**kwargs)

    def execute(self, context):
        with open("extract_twitter.json", "w") as output_file
          for page in TwitterHook(self.end_time, self.start_time, self.query).run():
            json.dump(page, output_file, ensure_ascii=False)   
            output_file.write("\n")