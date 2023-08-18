from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.microsoft.azure.transfers.local_to_wasb import LocalFilesystemToWasbOperator
from airflow.models import Variable

from airflow.hooks.base_hook import BaseHook

from datetime import datetime
import os
import json
import google_auth_oauthlib.flow
import googleapiclient.discovery
import googleapiclient.errors

# scopes = ["https://www.googleapis.com/auth/youtube.readonly"]




def _extract_channels():
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
        maxResults=200,
        q='data engineering',
        type='channel'
    )
    response = request.execute()

    with open("channels_info.json", 'w') as f:
        json.dump(response, f, indent=4)

    return response

    
with DAG(
    "yt-api",
    start_date=datetime(2023, 8, 14),
    schedule_interval="@daily",
    catchup=False
) as dag:

    read_channels = PythonOperator(
        task_id='google',
        python_callable=_extract_channels
    )

    # Get connection to Azure Blob Storage defined in Airflow UI
    conn = BaseHook.get_connection('azure-sa')

    save_channels = LocalFilesystemToWasbOperator(
        task_id="upload_file_to_Azure_Blob",
        file_path="channels_info.json",
        container_name="mol/yt-data",
        blob_name=f"channels-info-{datetime.now().strftime('%Y-%m-%d')}.json",
        wasb_conn_id=conn.conn_id
    )

    read_channels >> save_channels