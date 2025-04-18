import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd

# STEP 1: Auth
client_credentials_manager = SpotifyClientCredentials(
    client_id='',
    client_secret=''
)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

# STEP 2: Use user-created playlist instead of restricted editorial ones
playlist_link = "https://open.spotify.com/playlist/4zzUm9eZmeb4t4nUCaNoo5"  # ✅ Your playlist
playlist_URI = playlist_link.split("/")[-1].split('?')[0]

# STEP 3: Fetch all tracks
results = sp.playlist_tracks(playlist_URI)
tracks = results['items']
while results['next']:
    results = sp.next(results)
    tracks.extend(results['items'])

# STEP 4: Create lists
artist_list = []
song_list = []
album_list = []

for row in tracks:
    track = row['track']
    
    # Artist
    for artist in track['artists']:
        artist_dict = {
            'artist_id': artist['id'],
            'artist_name': artist['name'],
            'external_url': artist['href']
        }
        artist_list.append(artist_dict)
    
    # Album
    album = track['album']
    album_dict = {
        'album_id': album['id'],
        'album_name': album['name'],
        'release_date': album['release_date'],
        'total_tracks': album['total_tracks'],
        'external_url': album['href']
    }
    album_list.append(album_dict)

    # Song
    song_element = {
        'song_id': track['id'],
        'song_name': track['name'],
        'duration_ms': track['duration_ms'],
        'url': track['external_urls']['spotify'],
        'popularity': track['popularity'],
        'song_added': row['added_at'],
        'album_id': album['id'],
        'artist_id': track['artists'][0]['id']  # Primary artist
    }
    song_list.append(song_element)

# STEP 5: Convert to DataFrames
artist_df = pd.DataFrame(artist_list).drop_duplicates(subset='artist_id')
album_df = pd.DataFrame(album_list).drop_duplicates(subset='album_id')
song_df = pd.DataFrame(song_list)

# STEP 6: Convert dates
album_df['release_date'] = pd.to_datetime(album_df['release_date'], errors='coerce')
song_df['song_added'] = pd.to_datetime(song_df['song_added'])

# STEP 7: Show preview
print("\n🎤 Artists")
print(artist_df.head())

print("\n💿 Albums")
print(album_df.head())

print("\n🎵 Songs")
print(song_df.head())
