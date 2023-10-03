import pandas as pd 
import requests
from datetime import datetime
import datetime
import pandas as pd 
import requests
from datetime import datetime
import datetime

USER_ID = "Tavneet" 
TOKEN = "BQDfWr1yj0lDCxkidruS0wqIfz-ldeztQA6OFiTh__iEDNxQgg3jNBGNKNM7amZFlOD2UzKkFPFrqTh_-ZdCYdzMClLFw-A59EiqYKsP9sz0GefYZD9FNQCJ"

# Creating a function to be used in other Python files
def return_dataframe(playlist_url):  # Add playlist_url as a parameter
    input_variables = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer {token}".format(token=TOKEN)
    }
     
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=2)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000
    playlist_id = playlist_url.split("/")[-1]
    playlist_tracks_url = f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks"
    r = requests.get(playlist_tracks_url, headers=input_variables)

    if r.status_code != 200:
        print("Failed to retrieve data from Spotify API.")
        return None

    data = r.json()
    #print(data)
    song_names = []
    artist_names = []

    for track in data["items"]:
        song_names.append(track["track"]["name"])
        artist_names.append(track["track"]["artists"][0]["name"])

    song_dict = {
        "song_name": song_names,
        "artist_name": artist_names
    }
    song_df = pd.DataFrame(song_dict, columns=["song_name", "artist_name"])
    return song_df

def Data_Quality(load_df):
    # Checking Whether the DataFrame is empty
    if load_df.empty:
        print('No Songs Extracted')
        return False

    # Enforcing Primary keys since we don't need duplicates
    if load_df['song_name'].is_unique:
        pass
    else:
        # The Reason for using an exception is to immediately terminate the program and avoid further processing
        raise Exception("Primary Key Exception, Data Might Contain duplicates")

    # Checking for Nulls in our data frame
    if load_df.isnull().values.any():
        raise Exception("Null values found")

# Writing some Transformation Queries to get the count of artist
def Transform_df(load_df):
    # Applying transformation logic
    Transformed_df = load_df.groupby([ 'artist_name'], as_index=False)['song_name'].count()
    Transformed_df.rename(columns={'song_name': 'count'}, inplace=True)

    return Transformed_df

def spotify_etl():
    load_df=return_dataframe(playlist_url="https://api.spotify.com/v1/playlists/3cEYpjA9oz9GiPac4AsH4n")
    Data_Quality(load_df)
    ##calling the tranformation

    Transformed_df=Transform_df(load_df)
    print(load_df)
    return(load_df)


spotify_etl()


    