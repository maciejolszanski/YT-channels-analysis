from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.azure.transfers.local_to_wasb import LocalFilesystemToWasbOperator
from airflow.models import Variable

from airflow.hooks.base_hook import BaseHook

from datetime import datetime
import os
import json
import googleapiclient.discovery
import googleapiclient.errors

CHANNELS_TO_SEARCH = 'data engineering'

def _create_youtube_object():
    # Disable OAuthlib's HTTPS verification when running locally.
    # *DO NOT* leave this option enabled in production.
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

    api_service_name = "youtube"
    api_version = "v3"
    key_env = Variable.get("YT_KEY")

    # Create youtube object
    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey=key_env)
    
    return youtube

def _extract_channels():
    
    youtube = _create_youtube_object()

    # Search youtube channels with phrase defined as q param
    request = youtube.search().list(
        part="snippet",
        maxResults=200,
        q=CHANNELS_TO_SEARCH,
        type='channel'
    )
    response = request.execute()

    with open("channels_info.json", 'w') as f:
        json.dump(response, f, indent=4)

    return response


def _extract_videos(task_instance):
    full_response = task_instance.xcom_pull(task_ids='google')
    channels_data = full_response['items']
    channels_ids = [channel['snippet']['channelId'] for channel in channels_data]

    youtube = _create_youtube_object()

    for channel_id in channels_ids:
        request = youtube.search().list(
        part="snippet",
        maxResults=200,
        q=CHANNELS_TO_SEARCH,
        type='channel'
    )
    response = request.execute()

    
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
    blob_name = f"channels-{CHANNELS_TO_SEARCH}\
        -{datetime.now().strftime('%Y-%m-%d')}.json"

    save_channels = LocalFilesystemToWasbOperator(
        task_id="upload_file_to_Azure_Blob",
        file_path="channels_info.json",
        container_name="mol/yt-data/search-results",
        blob_name=blob_name,
        wasb_conn_id=conn.conn_id
    )

    read_channels >> save_channels