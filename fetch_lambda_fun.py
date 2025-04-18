import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime

def lambda_handler(event, context):
    
    # ✅ Corrected spelling
    client_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')
    
    # ✅ Auth
    client_credentials_manager = SpotifyClientCredentials(
        client_id=client_id,
        client_secret=client_secret
    )
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    
    # ✅ Use YOUR user-created playlist
    playlist_link = "https://open.spotify.com/playlist/4zzUm9eZmeb4t4nUCaNoo5"
    playlist_URI = playlist_link.split("/")[-1].split("?")[0]
    
    # ✅ Get playlist data
    spotify_data = sp.playlist_tracks(playlist_URI)
    
    # ✅ Connect to S3
    s3_client = boto3.client('s3')
    
    # ✅ Clean filename
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"spotify_raw_{timestamp}.json"
    
    # ✅ Upload to S3
    s3_client.put_object(
        Bucket="spotify-etl-ani",  
        Key=f"raw_data/to_processed/{filename}",
        Body=json.dumps(spotify_data, default=str)
    )

    return {
        "statusCode": 200,
        "body": f"Uploaded {filename} to S3 successfully ✅"
    }
