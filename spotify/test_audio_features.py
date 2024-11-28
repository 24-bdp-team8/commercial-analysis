from dotenv import load_dotenv
import os
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from pprint import pprint
import pandas as pd
import time

load_dotenv()

c_id = os.environ.get("CLIENT_ID")
c_secret = os.environ.get("CLIENT_SECRET")
redirect_uri = "http://localhost:8080/callback"

sp = spotipy.Spotify(
    auth_manager=SpotifyOAuth(
        client_id=c_id,
        client_secret=c_secret,
        redirect_uri=redirect_uri,
        scope="user-read-private",
    )
)

df = pd.read_csv("track_uris.csv")

track_ids = df["track_uri"].tolist()

audio_feature = sp.audio_features(track_ids[0])

pprint(audio_feature)
