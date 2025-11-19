from googleapiclient.discovery import build
import os
import pandas as pd
import warnings
import isodate
from sqlalchemy import create_engine, text
from datetime import datetime

warnings.filterwarnings("ignore")

# -------------------------------------------------------------------
# Logging Helper
# -------------------------------------------------------------------
def log(msg):
    print(f"[{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC] {msg}")

# -------------------------------------------------------------------
# Load Secrets
# -------------------------------------------------------------------
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_HOST = os.environ.get("DB_HOST")
DB_NAME = os.environ.get("DB_NAME")
API_KEY = os.environ.get("YOUTUBE_API_KEY")

# -------------------------------------------------------------------
# DB Connection
# -------------------------------------------------------------------
engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"
)

# -------------------------------------------------------------------
# YouTube API
# -------------------------------------------------------------------
youtube = build("youtube", "v3", developerKey=API_KEY)

channel_ids = [
    "UCY4rE2X-n2-TM_4K65CfXew","UCFuxLOUo41P3eEAW8U-Dwjg",
    "UCDRA2X1Tp2idmQZ4-EASDEA","UCiw4XPoqiJ4XVSUYOk0k7xQ",
    "UCuLftLIRZ2hHsDcLjwDo4Iw","UCIKAsBdkwH3ERoJd3hJQ-Jg",
    "UCQv19ogR1hFJ1tqPBGZBxDA","UCn8FN2lzzZIK41C3M4vb5Tw",
    "UCTP9sDeqayLKuD4QjHKdcIw","UC82KxaZyYynHpX3q1TQi_5g",
    "UCgilhqoyZrXV2EBlaaAtVaw","UCGeGhS_akOxBWQcSmje6B-w",
    "UCD8CFS_nj2_dBdSZu53wCcQ"
]

# -------------------------------------------------------------------
# Fetch channel stats
# -------------------------------------------------------------------
def get_channel_stats():
    log("Fetching channel stats...")
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=",".join(channel_ids)
    )
    response = request.execute()

    rows = []
    for item in response["items"]:
        rows.append({
            "channel_title": item["snippet"]["title"],
            "published_date": item["snippet"]["publishedAt"],
            "country": item["snippet"].get("country"),
            "subscribers": item["statistics"]["subscriberCount"],
            "views": item["statistics"]["viewCount"],
            "totalVideos": item["statistics"]["videoCount"],
            "playlistId": item["contentDetails"]["relatedPlaylists"]["uploads"]
        })

    df = pd.DataFrame(rows)
    log(f"Fetched {len(df)} channels.")
    return df

# -------------------------------------------------------------------
# Fetch video IDs
# -------------------------------------------------------------------
def get_video_ids(playlist_ids):
    log("Fetching video IDs...")

    all_data = []

    for pid in playlist_ids:
        next_page_token = None

        while True:
            request = youtube.playlistItems().list(
                part="contentDetails",
                playlistId=pid,
                maxResults=50,
                pageToken=next_page_token
            )
            response = request.execute()

            for item in response["items"]:
                all_data.append(item["contentDetails"]["videoId"])

            next_page_token = response.get("nextPageToken")
            if not next_page_token:
                break

    log(f"Total video IDs fetched: {len(all_data)}")
    return list(set(all_data))  # remove duplicates

# -------------------------------------------------------------------
# Fetch video stats
# -------------------------------------------------------------------
def get_video_stats(video_ids):
    log(f"Fetching video stats for {len(video_ids)} videos...")

    all_data = []

    for i in range(0, len(video_ids), 50):
        chunk = video_ids[i:i+50]
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=",".join(chunk)
        )
        response = request.execute()

        for item in response["items"]:
            all_data.append({
                "videoId": item["id"],
                "channel": item["snippet"].get("channelTitle"),
                "video_title": item["snippet"].get("title"),
                "description": item["snippet"].get("description"),
                "tags": item["snippet"].get("tags"),
                "publishedAt": item["snippet"].get("publishedAt"),
                "likes": item["statistics"].get("likeCount"),
                "views": item["statistics"].get("viewCount"),
                "comments": item["statistics"].get("commentCount"),
                "favourites": item["statistics"].get("favoriteCount"),
                "duration": item["contentDetails"].get("duration")
            })

    df = pd.DataFrame(all_data)
    log(f"Fetched stats for {len(df)} videos.")
    return df

# -------------------------------------------------------------------
# Main ETL
# -------------------------------------------------------------------
log("ETL job started.")

# 1. Channel stats (always replace)
channel_data = get_channel_stats()

# 2. Video IDs
video_ids = get_video_ids(channel_data["playlistId"].tolist())

# 3. Remove video IDs already in DB (incremental)
log("Checking for new videos (incremental filtering)...")

with engine.connect() as conn:
    existing = pd.read_sql("SELECT videoid FROM video_stats", conn) \
               if engine.dialect.has_table(conn, "video_stats") else pd.DataFrame()

existing_ids = set(existing["videoId"]) if not existing.empty else set()

new_video_ids = [vid for vid in video_ids if vid not in existing_ids]

log(f"New videos found: {len(new_video_ids)}")

if len(new_video_ids) == 0:
    log("No new videos. ETL finished.")
else:
    # 4. Fetch stats for NEW videos only
    video_stats = get_video_stats(new_video_ids)

    # --- Transformations ---
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

    video_stats["duration_sec"] = video_stats["duration"].apply(
        lambda x: isodate.parse_duration(x).total_seconds()
    )

    # ratios
    video_stats["comment_view_ratio"] = video_stats["comments"] / video_stats["views"] * 1000
    video_stats["like_view_ratio"] = video_stats["likes"] / video_stats["views"] * 1000

    # 5. Upload to DB
    log("Uploading new video stats...")
    video_stats.to_sql("video_stats", engine, if_exists="append", index=False)

# channel_stats: always replace
log("Updating channel_stats...")
channel_data.to_sql("channel_stats", engine, if_exists="replace", index=False)

log("ETL completed successfully!")
