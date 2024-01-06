import spotipy
import pandas as pd
from spotipy.oauth2 import SpotifyClientCredentials
from credentials import CLIENT_ID, CLIENT_SECRET

# Function to get a Spotify client using the provided client credentials
def get_spotify_client():
    client_credentials_manager = SpotifyClientCredentials(client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
    return spotipy.Spotify(client_credentials_manager=client_credentials_manager)

# Function to get playlist data from a Spotify playlist link
def get_playlist_data(playlist_link):
    sp = get_spotify_client()
    playlist_uri = playlist_link.split("/")[-1]
    return sp.playlist_tracks(playlist_uri)['items']

# Function to extract album data from a row of playlist data
def extract_album_data(row):
    album_info = row['track']['album']
    return {
        'album_id': album_info['id'],
        'album_name': album_info['name'],
        'release_date': album_info['release_date'],
        'album_total_tracks': album_info['total_tracks'],
        'album_url': album_info['external_urls']['spotify']
    }

# Function to extract artist data from a row of playlist data
def extract_artist_data(row):
    artist_info = row['track']['album']['artists'][0]
    artists_list = row['track']['artists']
    artist_names = ', '.join(artist['name'] for artist in artists_list)
    return {
        'artist_id': artist_info['id'],
        'artist_names': artist_names,
        'external_url': artist_info['external_urls']['spotify']
    }

# Function to extract song data from a row of playlist data
def extract_song_data(row):
    track_info = row['track']
    added_at = pd.to_datetime(row['added_at']).strftime('%Y-%m-%d %H:%M:%S')
    return {
        'song_id': track_info['id'],
        'song_name': track_info['name'],
        'song_duration': track_info['duration_ms'],
        'song_url': track_info['external_urls']['spotify'],
        'song_popularity': track_info['popularity'],
        'song_added': added_at,
        'album_id': track_info['album']['id'],
        'artist_id': track_info['album']['artists'][0]['id']
    }

# Function to fetch playlist data and save it as CSV files
def fetch_and_save_data(playlist_link):
    playlist_data = get_playlist_data(playlist_link)

    # Extract album data and save to CSV
    album_list = [extract_album_data(row) for row in playlist_data]
    df_album = pd.DataFrame(album_list)
    df_album.to_csv('data/album.csv', index=False)

    # Extract artist data and save to CSV
    artist_list = [extract_artist_data(row) for row in playlist_data]
    df_artist = pd.DataFrame(artist_list)
    df_artist.to_csv('data/artist.csv', index=False)

    # Extract song data and save to CSV
    song_list = [extract_song_data(row) for row in playlist_data]
    df = pd.DataFrame(song_list)
    df['song_added'] = pd.to_datetime(df['song_added'])
    df.to_csv('data/song.csv', index=False)

    #Execute the data fetching and saving when the script is run
if __name__ == "__main__":
    playlist_link = "https://open.spotify.com/playlist/37i9dQZF1DX802IXCAaWtY"
    fetch_and_save_data(playlist_link)
