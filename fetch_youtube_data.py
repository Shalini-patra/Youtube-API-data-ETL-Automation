from googleapiclient.discovery import build
import os
import json
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings("ignore")

from sqlalchemy import create_engine
import isodate

# ------------------------------------------------------------------------------
# Fetch DB and API credentials
# ------------------------------------------------------------------------------
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_HOST = os.environ.get("DB_HOST")
DB_NAME = os.environ.get("DB_NAME")

API_KEY = os.environ.get("YOUTUBE_API_KEY")

# ------------------------------------------------------------------------------
# SQLAlchemy engine
# ------------------------------------------------------------------------------
engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"
)

# ------------------------------------------------------------------------------
# YouTube API setup
# ------------------------------------------------------------------------------
api_service_name = "youtube"
api_version = "v3"

youtube = build(api_service_name, api_version, developerKey=API_KEY)

# ------------------------------------------------------------------------------
# Channel IDs
# ------------------------------------------------------------------------------
channel_ids = [
    "UCY4rE2X-n2-TM_4K65CfXew","UCFuxLOUo41P3eEAW8U-Dwjg",
    "UCDRA2X1Tp2idmQZ4-EASDEA","UCiw4XPoqiJ4XVSUYOk0k7xQ",
    "UCuLftLIRZ2hHsDcLjwDo4Iw","UCIKAsBdkwH3ERoJd3hJQ-Jg",
    "UCQv19ogR1hFJ1tqPBGZBxDA","UCn8FN2lzzZIK41C3M4vb5Tw",
    "UCTP9sDeqayLKuD4QjHKdcIw","UC82KxaZyYynHpX3q1TQi_5g",
    "UCgilhqoyZrXV2EBlaaAtVaw","UCGeGhS_akOxBWQcSmje6B-w",
    "UCD8CFS_nj2_dBdSZu53wCcQ"
]

# ------------------------------------------------------------------------------
# Fetch channel stats
# ------------------------------------------------------------------------------
def get_channel_stats(youtube, channel_ids):
    all_data = []

    request = youtube.channels().list(
        part="snippet,contentDetails,statistics,status",
        id=",".join(channel_ids)
    )
    response = request.execute()

    for item in response["items"]:
        data = {
            "channel_title": item["snippet"]["title"],
            "published_date": item["snippet"]["publishedAt"],
            "country": item["snippet"].get("country"),
            "subscribers": item["statistics"]["subscriberCount"],
            "views": item["statistics"]["viewCount"],
            "totalVideos": item["statistics"]["videoCount"],
            "playlistId": item["contentDetails"]["relatedPlaylists"]["uploads"]
        }
        all_data.append(data)

    return pd.DataFrame(all_data)

# ------------------------------------------------------------------------------
# Fetch video IDs
# ------------------------------------------------------------------------------
def get_video_ids(youtube, playlist_ids):
    all_data = []

    for pid in playlist_ids:
        next_page_token = None

        while True:
            request = youtube.playlistItems().list(
                part="contentDetails,snippet",
                playlistId=pid,
                maxResults=50,
                pageToken=next_page_token
            )
            response = request.execute()

            for item in response["items"]:
                data = {
                    "channelId": item["snippet"]["channelId"],
                    "playlistId": item["snippet"]["playlistId"],
                    "video_id": item["contentDetails"]["videoId"]
                }
                all_data.append(data)

            next_page_token = response.get("nextPageToken")
            if not next_page_token:
                break

    return pd.DataFrame(all_data)

# ------------------------------------------------------------------------------
# Fetch video stats
# ------------------------------------------------------------------------------
def get_video_stats(youtube, video_id_list):
    all_data = []

    for i in range(0, len(video_id_list), 50):
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=",".join(video_id_list[i:i + 50])
        )
        response = request.execute()

        for item in response["items"]:
            data = {
                "channel": item["snippet"].get("channelTitle"),
                "videoId": item["id"],
                "video_title": item["snippet"].get("title"),
                "description": item["snippet"].get("description"),
                "tags": item["snippet"].get("tags"),
                "publishedAt": item["snippet"].get("publishedAt"),
                "likes": item["statistics"].get("likeCount"),
                "views": item["statistics"].get("viewCount"),
                "comments": item["statistics"].get("commentCount"),
                "favourites": item["statistics"].get("favoriteCount"),
                "duration": item["contentDetails"].get("duration"),
            }
            all_data.append(data)

    return pd.DataFrame(all_data)

# ------------------------------------------------------------------------------
# ETL Workflow (runs ONCE per GitHub Action run)
# ------------------------------------------------------------------------------
channel_data = get_channel_stats(youtube, channel_ids)

playlist_ids = channel_data["playlistId"].tolist()
video_ids = get_video_ids(youtube, playlist_ids)

video_id_list = video_ids["video_id"].tolist()
video_stats = get_video_stats(youtube, video_id_list)

# --- Transformations ---
channel_data["subscribers"] = channel_data["subscribers"].astype(int)
channel_data["views"] = channel_data["views"].astype(int)
channel_data["totalVideos"] = channel_data["totalVideos"].astype(int)
channel_data["channel_published"] = pd.to_datetime(channel_data["published_date"]).dt.date

video_stats["views"] = video_stats["views"].astype(int)
video_stats["likes"] = video_stats["likes"].fillna(0).astype(int)
video_stats["comments"] = video_stats["comments"].fillna(0).astype(int)
video_stats["favourites"] = video_stats["favourites"].fillna(0).astype(int)

video_stats["tag_count"] = video_stats["tags"].apply(lambda x: len(x) if x else 0)
video_stats["description_length"] = video_stats["description"].apply(lambda x: len(x) if x else 0)

video_stats["publishedAt"] = pd.to_datetime(video_stats["publishedAt"], errors="coerce")
video_stats["publish_year"] = video_stats["publishedAt"].dt.year
video_stats["publish_time"] = video_stats["publishedAt"].dt.time
video_stats["published_dayofweek"] = video_stats["publishedAt"].dt.day_name()
video_stats["duration_sec"] = video_stats["duration"].apply(lambda x: isodate.parse_duration(x).total_seconds())

# ratios
video_stats["comment_view_ratio"] = video_stats["comments"] / video_stats["views"] * 1000
video_stats["like_view_ratio"] = video_stats["likes"] / video_stats["views"] * 1000

# ------------------------------------------------------------------------------
# Load into PostgreSQL
# ------------------------------------------------------------------------------
video_stats.to_sql("video_stats", engine, if_exists="replace", index=False)
channel_data.to_sql("channel_stats", engine, if_exists="replace", index=False)

print("ETL Updated Successfully!")
