import json
import boto3
from datetime import datetime
from io import StringIO
import pandas as pd

# ---------- Extraction Functions ----------
def album(data):
    album_list = []
    for row in data['items']:
        album_info = row['track']['album']
        album_element = {
            'album_id': album_info['id'],
            'name': album_info['name'],
            'release_date': album_info['release_date'],
            'total_tracks': album_info['total_tracks'],
            'url': album_info['external_urls']['spotify']
        }
        album_list.append(album_element)
    return album_list

def artist(data):
    artist_list = []
    for row in data['items']:
        for artist in row['track']['artists']:
            artist_dict = {
                'artist_id': artist['id'],
                'artist_name': artist['name'],
                'external_url': artist['href']
            }
            artist_list.append(artist_dict)
    return artist_list

def songs(data):
    song_list = []
    for row in data['items']:
        track = row['track']
        song_element = {
            'song_id': track['id'],
            'song_name': track['name'],
            'duration_ms': track['duration_ms'],
            'url': track['external_urls']['spotify'],
            'popularity': track['popularity'],
            'song_added': row['added_at'],
            'album_id': track['album']['id'],
            'artist_id': track['album']['artists'][0]['id']
        }
        song_list.append(song_element)
    return song_list

# ---------- Lambda Handler ----------
def lambda_handler(event, context):
    s3 = boto3.client('s3')
    bucket = "spotify-etl-ani"
    prefix = "raw_data/to_processed/"
    
    spotify_data = []
    spotify_keys = []
    
    # Read raw JSONs from S3
    for file in s3.list_objects(Bucket=bucket, Prefix=prefix)['Contents']:
        key = file['Key']
        if key.endswith(".json"):
            response = s3.get_object(Bucket=bucket, Key=key)
            content = response['Body']
            jsonObject = json.loads(content.read())
            spotify_data.append(jsonObject)
            spotify_keys.append(key)

    # Transform and upload
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    for data in spotify_data:
        # Transform
        album_df = pd.DataFrame(album(data)).drop_duplicates(subset=['album_id'])
        artist_df = pd.DataFrame(artist(data)).drop_duplicates(subset=['artist_id'])
        song_df = pd.DataFrame(songs(data))

        # Fix date parsing
        album_df['release_date'] = pd.to_datetime(album_df['release_date'], errors='coerce')
        song_df['song_added'] = pd.to_datetime(song_df['song_added'], errors='coerce')

        # Upload songs
        song_key = f"transformed_data/songs_data/songs_transformed_{timestamp}.csv"
        song_buffer = StringIO()
        song_df.to_csv(song_buffer, index=False)
        s3.put_object(Bucket=bucket, Key=song_key, Body=song_buffer.getvalue())

        # Upload albums
        album_key = f"transformed_data/album_data/album_transformed_{timestamp}.csv"
        album_buffer = StringIO()
        album_df.to_csv(album_buffer, index=False)
        s3.put_object(Bucket=bucket, Key=album_key, Body=album_buffer.getvalue())

        # Upload artists
        artist_key = f"transformed_data/artist_data/artist_transformed_{timestamp}.csv"
        artist_buffer = StringIO()
        artist_df.to_csv(artist_buffer, index=False)
        s3.put_object(Bucket=bucket, Key=artist_key, Body=artist_buffer.getvalue())

    # Archive raw files
    s3_resource = boto3.resource('s3')
    for key in spotify_keys:
        copy_source = {'Bucket': bucket, 'Key': key}
        processed_key = "raw_data/processed/" + key.split("/")[-1]
        s3_resource.meta.client.copy(copy_source, bucket, processed_key)
        s3_resource.Object(bucket, key).delete()

    return {
        "statusCode": 200,
        "body": f"Successfully transformed and uploaded files at {timestamp}"
    }
