from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.azure.transfers.local_to_wasb import LocalFilesystemToWasbOperator
from airflow.models import Variable

from airflow.hooks.base_hook import BaseHook

from datetime import datetime
import copy
import os
import json
import googleapiclient.discovery
import googleapiclient.errors

CHANNELS_TO_SEARCH = 'data engineering'


def _create_youtube_object():
    """
    Description:
        This function creates and returns a youtube object,
        that can be further used for sending API requests.

    Output:
        youtube  -> googleapiclient.disvocery -> Youtube object.
    """

    api_service_name = "youtube"
    api_version = "v3"
    key_env = Variable.get("YT_KEY")

    # Create youtube object
    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey=key_env)
    
    return youtube

def _extract_search_data():
    """
    Description:
        This function requests top 200 search results for 
        phrase defined in global variable CHANNELS_TO_SEARCH. 
        Results are saved locally as "search-data.json" file
        as well as returned by this function.

    Output:
        search_results -> dict -> Youtube API response containig
                                  data about top 200 search results.
    """

    youtube = _create_youtube_object()

    # Search youtube channels with phrase defined as q param
    request = youtube.search().list(
        part="snippet",
        maxResults=200,
        q=CHANNELS_TO_SEARCH,
        type='channel'
    )
    search_results = request.execute()

    with open("search-data.json", 'w') as f:
        json.dump(search_results, f, indent=4)

    return search_results


def _extract_channel_stats(task_instance):
    """
    Description:
        This function retrieves channels ids from search results
        and then fetches channel statistics for each channel 
        in one request (Youtube Data API allows to specify 
        multiple channels ids as a comma separated string).
        Channel stats are saved locally as "channels-data.json" 
        as well as returned by this function.

    Input:
        task_instance -> Airflow task -> Instance of a task that we want to
                                         get returned data from. 

    Output:
        channels_data -> dict -> Youtube API response containig
                                data about top channels.
    """

    full_search_response = task_instance.xcom_pull(
        task_ids='get_search_data')
    channels_data = full_search_response['items']
    channels_ids = ','.join([channel['snippet']['channelId'] for channel in channels_data])

    youtube = _create_youtube_object()

    request = youtube.channels().list(
        part="contentDetails,statistics,topicDetails",
        id=channels_ids
    )
    channels_data = request.execute()

    with open("channels-data.json", 'w') as f:
        json.dump(channels_data, f, indent=4)
    
    return channels_data

def _get_video_ids(channels_data):
    """
    Description:
        This function iterates over playlists_ids that stores videos
        uploaded by channels and then appends all videos ids to 
        one list.

    Input:
        channels_data -> dict -> Dict containig data about playlists with
                                 uploaded videos.
    Output:
        videos_ids -> list -> List containig all videos ids.
    """

    youtube = _create_youtube_object()
    playlist_ids = [channel_data['contentDetails']['relatedPlaylists']['uploads'] for channel_data in channels_data]

    videos_ids = []

    for playlist in playlist_ids:

        request = youtube.playlistItems().list(
            part="contentDetails,id",
            playlistId=playlist,
            maxResults=200
        )

        playlist_data = request.execute()
        new_videos_ids = [video['contentDetails']['videoId'] for video in playlist_data['items']]

        videos_ids += new_videos_ids

    return videos_ids

def _extract_videos_data(task_instance):
    """
    Description:
        This function retrieves last 200 videos ids 
        upload by each channel and then fetches videos statistics.
        Request may contain max 500 video ids, so list containig
        them is divided into 500 element slices.
        Videos stats are saved locally as "videos-data.json" 
        as well as returned by this function.

    Input:
        task_instance -> Airflow task -> Instance of a task that we want to
                                         get returned data from. 

    Output:
        videos_data -> dict -> Youtube API response containig
                               data about videos uploaded by channels.
    """


    full_channels_data = task_instance.xcom_pull(task_ids='get_channels_data')
    channels_data = full_channels_data['items']
    video_ids = _get_video_ids(channels_data)

    output_videos_dict = {}
    
    # Youtube API doesn't allow to request more than 50 ids
    # Here I divide the list for 50 element slices
    # and request 50 ids multiple times
    for i in range(int(len(video_ids)/50)+1):

        video_ids_slice = video_ids[50*i:50*(i+1)]
        # This never happens, but to be more than 100% sure
        assert(len(video_ids_slice)> 0)

        youtube = _create_youtube_object()

        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_ids_slice
        )
        videos_data = request.execute()

        # Joining following requests responses into one dict
        if i == 0: 
            output_videos_dict = videos_data
        else:
            full_videos = output_videos_dict['items'] + videos_data['items']
            output_videos_dict['items'] = full_videos


    with open("videos-data.json", 'w') as f:
        json.dump(output_videos_dict, f, indent=4)

    return videos_data

    
with DAG(
    "yt-api",
    start_date=datetime(2023, 8, 14),
    schedule_interval="@daily",
    catchup=False
) as dag:

    get_search_data = PythonOperator(
        task_id='get_search_data',
        python_callable=_extract_search_data
    )

    get_channels_data = PythonOperator(
        task_id='get_channels_data',
        python_callable=_extract_channel_stats
    )

    get_videos_data = PythonOperator(
        task_id='get_videos_data',
        python_callable=_extract_videos_data
    )

    # Get connection to Azure Blob Storage defined in Airflow UI
    conn = BaseHook.get_connection('azure-sa')
    
    channels_with_dash = CHANNELS_TO_SEARCH.replace(' ', '-')
    search_blob_name = f"search-{channels_with_dash}-{datetime.now().strftime('%Y-%m-%d-%H%M%S')}.json"
    channels_blob_name = f"channels-{channels_with_dash}-{datetime.now().strftime('%Y-%m-%d-%H%M%S')}.json"
    videos_blob_name = f"videos-{channels_with_dash}-{datetime.now().strftime('%Y-%m-%d-%H%M%S')}.json"

    save_search_data = LocalFilesystemToWasbOperator(
        task_id="upload_search_data_to_Azure_Blob",
        file_path="search-data.json",
        container_name="mol/yt-data/search",
        blob_name=search_blob_name,
        wasb_conn_id=conn.conn_id
    )

    save_channels_data = LocalFilesystemToWasbOperator(
        task_id="upload_channels_data_to_Azure_Blob",
        file_path="channels-data.json",
        container_name="mol/yt-data/channels",
        blob_name=channels_blob_name,
        wasb_conn_id=conn.conn_id
    )

    save_videos_data = LocalFilesystemToWasbOperator(
        task_id="upload_videos_data_to_Azure_Blob",
        file_path="videos-data.json",
        container_name="mol/yt-data/videos",
        blob_name=videos_blob_name,
        wasb_conn_id=conn.conn_id
    )

    get_search_data >> get_channels_data >> get_videos_data >> [save_search_data, save_channels_data, save_videos_data]