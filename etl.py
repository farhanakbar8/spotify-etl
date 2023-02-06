import requests
import os
from dotenv import load_dotenv
import datetime
import pandas as pd
import mysql.connector

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

def load_data(data, table):
    try:
        connection = mysql.connector.connect(
            user=os.getenv('USER'),
            password=os.getenv('PASSWORD'),
            host=os.getenv('HOST'),
            database=os.getenv('DATABASE'),
            autocommit=True
        )
    except mysql.connector.Error as error:
        print(error)

    cursor = connection.cursor()

    query = ""
    for i, item in data.iterrows():
        query += f"INSERT INTO {table} (songs, artists, time_played, dates) VALUES (\"{item['songs']}\", \"{item['artists']}\", \"{item['time_played']}\", \"{item['dates']}\");\n"

    try:
        cursor.execute(query)
        print("Data Succesfully loaded to the Database")
    except mysql.connector.Error as error:
        print(error)
        connection.rollback()
    
    connection.close()

if __name__ == "__main__":
    data = extract_data(os.getenv('TOKEN'), 1)
    data = transform_data(data)
    load_data(data, os.getenv('TABLE_NAME'))