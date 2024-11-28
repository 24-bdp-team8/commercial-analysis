from dotenv import load_dotenv
import os
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from pprint import pprint
import pandas as pd
import time

def extract_track_info(track, audio_features):
    return {
        "track_id": track["id"],
        "track_name": track["name"],
        "track_artist": track["artists"][0]["name"],
        "track_url": track["external_urls"]["spotify"],
        "acousticness": audio_features["acousticness"],
        "danceability": audio_features["danceability"],
        "energy": audio_features["energy"],
        "instrumentalness": audio_features["instrumentalness"],
        "key": audio_features["key"],
        "liveness": audio_features["liveness"],
        "loudness": audio_features["loudness"],
        "mode": audio_features["mode"],
        "speechiness": audio_features["speechiness"],
        "tempo": audio_features["tempo"],
        "valence": audio_features["valence"]
    }

load_dotenv()

c_id = os.environ.get("CLIENT_ID")
c_secret = os.environ.get("CLIENT_SECRET")
redirect_uri = "http://localhost:8080/callback"

sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=c_id, client_secret=c_secret, redirect_uri=redirect_uri))

data = []
offset = 0
year = 2023
limit = 50

while True:
    try:
        result = sp.search(q=f"year:{year}", type="track", market="KR", limit=limit, offset=offset)
        print(offset)

        track_items = result["tracks"]["items"]
    
        for track in track_items:
            data.append({
                "track_id": track["id"],
                "track_name": track["name"],
                "track_artist": track["artists"][0]["name"],
                "track_url": track["external_urls"]["spotify"],
            })

        time.sleep(1)
        offset += limit
    except Exception as e:
        break
   
    # audio_features = sp.audio_features([item["track_id"] for item in data])
    # print("audio_features")

    # for i in range(len(data)):
    #     data[i].update({
    #         "acousticness": audio_features[i]["acousticness"],
    #         "danceability": audio_features[i]["danceability"],
    #         "energy": audio_features[i]["energy"],
    #         "instrumentalness": audio_features[i]["instrumentalness"],
    #         "key": audio_features[i]["key"],
    #         "liveness": audio_features[i]["liveness"],
    #         "loudness": audio_features[i]["loudness"],
    #         "mode": audio_features[i]["mode"],
    #         "speechiness": audio_features[i]["speechiness"],
    #         "tempo": audio_features[i]["tempo"],
    #         "valence": audio_features[i]["valence"]
    #     })

    # time.sleep(15)

df = pd.DataFrame(
    data, 
    columns=["track_id", "track_name", "track_artist", "track_url"]
    # "acousticness", "danceability", "energy", "instrumentalness", "key", "liveness", "loudness", "mode", "speechiness", "tempo", "valence"
)
df.to_csv(f"sample_data/tracks_{year}.csv", index=False)
df.to_parquet(f"sample_data/tracks_{year}.parquet", index=False)
