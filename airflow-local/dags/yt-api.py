from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable

from datetime import datetime
import os
import google_auth_oauthlib.flow
import googleapiclient.discovery
import googleapiclient.errors

scopes = ["https://www.googleapis.com/auth/youtube.readonly"]

def _read_channels():
    # Disable OAuthlib's HTTPS verification when running locally.
    # *DO NOT* leave this option enabled in production.
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

    api_service_name = "youtube"
    api_version = "v3"
    key_env = Variable.get("YT_KEY")

    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey=key_env)

    request = youtube.search().list(
        part="snippet",
        maxResults=20,
        q='data engineering',
        type='channel'
    )
    response = request.execute()

    return response

    
with DAG(
    "yt-api",
    start_date=datetime(2023, 8, 14),
    schedule_interval="@daily",
    catchup=False
) as dag:

    read_channels = PythonOperator(
        task_id='google',
        python_callable=_read_channels
    )

    save_channels = PythonOperator(

    )