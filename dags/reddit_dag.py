import os
import sys
from datetime import datetime
from airflow.sdk import dag, task
from airflow.sdk.bases.operator import chain

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines.aws_s3_pipeline import upload_s3_pipeline
from pipelines.reddit_pipeline import reddit_pipeline


@dag(
    dag_id="etl_reddit_pipeline",
    default_args={
        "owner": "Fred Offei",
    },
    start_date=datetime(2023, 10, 22),
    schedule="@daily",
    catchup=False,
    tags=["reddit", "etl", "pipeline"],
)
def etl_reddit_pipeline():

    @task()
    def reddit_extraction():
        file_postfix = datetime.now().strftime("%Y%m%d")
        reddit_pipeline(
            file_name=f"reddit_{file_postfix}",
            subreddit="dataengineering",
            time_filter="day",
            limit=100,
        )

    @task()
    def s3_upload():
        upload_s3_pipeline()

    chain(reddit_extraction(), s3_upload())


etl_reddit_pipeline()
