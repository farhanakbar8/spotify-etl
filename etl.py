import requests
import os
from dotenv import load_dotenv
import datetime
import pandas as pd

load_dotenv()

def extract_data(token, days):
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }

    yesterday = int((datetime.datetime.now() - datetime.timedelta(days=days)).timestamp()) * 1000
    request = requests.get(f'https://api.spotify.com/v1/me/player/recently-played?before={yesterday}&limit=50', headers=headers)

    return request.json()

def transform_data(data):
    songs = []
    artists = []
    time_played = []
    dates = []

    for item in data['items']:
        songs.append(item['track']['name'])

        if len(item['track']['album']['artists']) == 1:
            artists.append(item['track']['album']['artists'][0]['name'])
        else:
            temp = []
            for a in item['track']['album']['artists']:
                temp.append(a['name'])
            artists.append(" & ".join(t for t in temp))
        
        time_played.append(item['played_at'].split('T')[1].split('.')[0])
        dates.append(item['played_at'].split('T')[0])

    data_dict = {
        "songs": songs,
        "artists": artists,
        "time_played": time_played,
        "dates": dates
    }

    return pd.DataFrame(data_dict)

if __name__ == "__main__":
    data = extract_data(os.getenv('TOKEN'), 1)
    data = transform_data(data)
    print(data)