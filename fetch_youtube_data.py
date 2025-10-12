from googleapiclient.discovery import build
import json
import pandas as pd
import numpy as np
from IPython.display import JSON
import warnings
warnings.filterwarnings("ignore")

##List of Youtube Channels

api_key="AIzaSyDPxXocOlA-fOp7eX2H9R4PxEYTW8VEdMA"
channel_ids=["UCY4rE2X-n2-TM_4K65CfXew","UCFuxLOUo41P3eEAW8U-Dwjg",
"UCDRA2X1Tp2idmQZ4-EASDEA","UCiw4XPoqiJ4XVSUYOk0k7xQ",
"UCuLftLIRZ2hHsDcLjwDo4Iw","UCIKAsBdkwH3ERoJd3hJQ-Jg",
"UCQv19ogR1hFJ1tqPBGZBxDA","UCn8FN2lzzZIK41C3M4vb5Tw",
"UCTP9sDeqayLKuD4QjHKdcIw","UC82KxaZyYynHpX3q1TQi_5g",
"UCgilhqoyZrXV2EBlaaAtVaw","UCGeGhS_akOxBWQcSmje6B-w",
"UCD8CFS_nj2_dBdSZu53wCcQ"]

##Fetching Channel Stats with Youtube API

api_service_name = "youtube"
api_version = "v3"
client_secrets_file = "YOUR_CLIENT_SECRET_FILE.json"

youtube = build(api_service_name, api_version, developerKey=api_key)

##get_channel_stats() 

all_data=[]
def get_channel_stats(youtube,channel_ids):
    all_data=[]
    request=youtube.channels().list(
        part="snippet, ContentDetails,statistics,status",
        id=','.join(channel_ids)
    )
    response=request.execute()

    for i in range(len(response['items'])):
      data={'channel_title':response['items'][i]['snippet']['title'],
            'published_date':response['items'][i]['snippet']['publishedAt'],
            'country':response['items'][i]['snippet']['country'],
            'subscribers':response['items'][i]['statistics']['subscriberCount'],
            'views' :response['items'][i]['statistics']['viewCount'],
            'totalVideos':response['items'][i]['statistics']['videoCount'],
            'playlistId':response['items'][i]['contentDetails']['relatedPlaylists']['uploads']}
      all_data.append(data)

    return pd.DataFrame(all_data)

##get_video_ids()
playlist_ids=channel_data['playlistId'].tolist()
def get_video_ids(youtube, playlist_ids):
    all_data = []

    for pid in playlist_ids:  # loop each playlist id
        next_page_token = None

        while True:
            request = youtube.playlistItems().list(
                part="contentDetails,snippet",
                playlistId=pid,
                maxResults=50,
                pageToken=next_page_token
            )
            response = request.execute()

            for item in response['items']:
                data = {
                    'channelId': item['snippet']['channelId'],
                    'playlistId': item['snippet']['playlistId'],
                    'video_id': item['contentDetails']['videoId']
                }
                all_data.append(data)

            next_page_token = response.get("nextPageToken")
            if not next_page_token:
                break

    return pd.DataFrame(all_data)
  
video_ids=get_video_ids(youtube,playlist_ids)

##get_video_stats()

video_id_list=video_ids['video_id'].to_list()
def get_video_stats(youtube,video_id_list):
    all_data=[]
    for i in range(0,len(video_id_list),50):
        request=youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=','.join(video_id_list[i:i+50])
        )
        response=request.execute()

        for item in response['items']:
          data = { 'channel': item['snippet'].get('channelTitle'),
                  'videoId': item['id'],
                   'video_title': item['snippet'].get('title'),
                   'description': item['snippet'].get('description'),
                   'tags': item['snippet'].get('tags'),
                   'publishedAt': item['snippet'].get('publishedAt'),
                   'likes': item['statistics'].get('likeCount'),
                   'views': item['statistics'].get('viewCount'),
                   'comments': item['statistics'].get('commentCount'),
                   'favourites': item['statistics'].get('favoriteCount'),
                   'duration': item['contentDetails'].get('duration') }

          all_data.append(data)

    return pd.DataFrame(all_data)

## Automate Data ETL Process

import time

while true:
  channel_data=get_channel_stats(youtube,channel_ids)
  video_ids=get_video_ids(youtube,playlist_ids)
  video_stats=get_video_stats(youtube,video_id_list)

  num_cols = ['subscribers', 'views', 'totalVideos']
  for col in num_cols:
    channel_data[col] = channel_data[col].astype(int)

  video_num_cols=['comments','views','likes','favourites']
  for col in video_num_cols:
    video_stats[col] = video_stats[col].fillna(0 ).astype(int)

  channel_data['channel_published'] = pd.to_datetime(channel_data['published_date'],errors='coerce').dt.date


  video_stats['tag_count']=video_stats['tags'].apply(lambda x:len(x) if x is not None else 0)
  video_stats['description_length']=video_stats['description'].apply(lambda x:len(x) if x is not None else 0)
  video_stats['publishedAt'] = pd.to_datetime(video_stats['publishedAt'], errors='coerce')
  video_stats['publish_year']=video_stats['publishedAt'].dt.year
  video_stats['publish_time']=video_stats['publishedAt'].dt.time
  video_stats['published_dayofweek'] = video_stats['publishedAt'].dt.day_name()
  video_stats['duration_sec']=video_stats['duration'].apply(lambda x: isodate.parse_duration(x).total_seconds())
  video_stats['comment-viewratio']=video_stats['comments']/video_stats['views']*1000
  video_stats['like-viewratio']=video_stats['likes']/video_stats['views']*1000
  video_stats_dropped = video_stats.drop(columns=['tags'])
  video_stats['publish_time2'] = video_stats['publish_time'].apply(lambda x: x.hour * 3600 + x.minute * 60 + x.second)
  video_stats.to_sql('video_stats', engine, if_exists='replace', index=False)
  channel_data.to_sql('channel_stats', engine, if_exists='replace', index=False)
  print("Updated successfully!")

  time.sleep(86400)


