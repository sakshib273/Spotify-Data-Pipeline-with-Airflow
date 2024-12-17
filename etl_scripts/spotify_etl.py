import configparser
import spotipy
import pandas as pd
from spotipy.oauth2 import SpotifyOAuth
import boto3
from datetime import datetime
import os
import pytz
import base64
import requests

# Load configuration

config_file_path = os.path.join(os.path.dirname(__file__), '../config.ini')

config = configparser.ConfigParser()
config.read(config_file_path)

sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
    client_id=config['spotify']['client_id'],
    client_secret=config['spotify']['client_secret'],
    redirect_uri=config['spotify']['redirect_uri'],
    scope=config['spotify']['scope']
))

CLIENT_ID = config['spotify']['client_id']
CLIENT_SECRET = config['spotify']['client_secret']
REFRESH_TOKEN = config['spotify']['refresh_tokden']
TOKEN_URL = 'https://accounts.spotify.com/api/token'

S3_BUCKET = config['aws']['s3_bucket']
S3_TRACKS_FOLDER = config['aws']['s3_tracks_folder']
S3_ARTISTS_FOLDER = config['aws']['s3_artists_folder']

def refresh_spotify_token():
    # Encode client credentials
    auth_header = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()

    # Data to send to Spotify
    data = {
        'grant_type': 'refresh_token',
        'refresh_token': REFRESH_TOKEN
    }

    headers = {
        'Authorization': f'Basic {auth_header}',
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    # Make the request to refresh the token
    response = requests.post(TOKEN_URL, headers=headers, data=data)

    if response.status_code == 200:
        # Successful response, extract the new access token
        new_access_token = response.json()['access_token']
        return new_access_token
    else:
        raise Exception(f"Failed to refresh token: {response.status_code} - {response.text}")

def create_spotify_client(access_token):
    return spotipy.Spotify(auth=access_token)  

def get_recently_played(sp):
    limit = int(config['etl']['limit'])  # Fetch limit from config.ini
    results = sp.current_user_recently_played(limit=limit)
    return results

def save_to_s3(df, bucket, folder, file_name):
    s3 = boto3.client(
        's3',
        aws_access_key_id=config['aws']['aws_access_key_id'],
        aws_secret_access_key=config['aws']['aws_secret_access_key']
    )
    csv_buffer = df.to_csv(index=False)
    s3.put_object(Bucket=bucket, Key=os.path.join(folder, file_name), Body=csv_buffer)


def run_etl():
    # Refresh token and create Spotify client
    new_access_token = refresh_spotify_token()
    sp = create_spotify_client(new_access_token)

    # Get recently played tracks
    results = get_recently_played(sp)

    track_data = []
    artist_data = []

    local_tz = pytz.timezone('Asia/Kolkata')
    utc_now = datetime.utcnow()  # Current UTC time
    local_now = datetime.now(local_tz)  # Current local time (Asia/Kolkata)
    today_str = local_now.strftime('%Y-%m-%d')  # Get current date in local time
    
    for item in results['items']:
        track = item['track']
        album = track['album']

        context = item.get('context', None)
        if context:
            external_urls = context.get('external_urls', {})
            playlist_url = external_urls.get('spotify', None)
            context_type = context.get('type', None)
        else:
            playlist_url = None
            context_type = None

        # Convert played_at from UTC to local timezone
        played_at_utc = pd.to_datetime(item['played_at'], errors='coerce')

        # Check if the timestamp is already timezone-aware (i.e., it already has UTC)
        if played_at_utc.tzinfo is None:
            played_at_utc = played_at_utc.tz_localize('UTC')
        else:
            played_at_utc = played_at_utc.tz_convert('UTC')

        played_at_local = played_at_utc.tz_convert(local_tz)
        played_at_date = played_at_local.strftime('%Y-%m-%d')

        # Only collect data for tracks played today in local time
        if played_at_date == today_str:  # Check if the track was played today
            track_data.append({
                'track_id': track['id'],
                'track_name': track['name'],
                'album_artist_id': track['artists'][0]['id'],
                'album_artist_name': track['artists'][0]['name'],
                'duration_ms': track['duration_ms'],
                'explicit': track['explicit'],
                'popularity': track['popularity'],
                'track_number': track['track_number'],
                'played_at': played_at_local,  # Use the localized version
                'track_url': track['external_urls']['spotify'],
                'artist_url': track['artists'][0]['external_urls']['spotify'],
                'album_id': album['id'],
                'album_name': album['name'],
                'album_release_date': album['release_date'],
                'total_tracks': album['total_tracks'],
                'album_url': album['external_urls']['spotify'],
                'context_type': context_type,
                'playlist_url': playlist_url
            })

            # Add artist data
            for artist in track['artists']:
                artist_data.append({
                    'track_id': track['id'],
                    'artist_id': artist['id'],
                    'artist_name': artist['name'], 
                })

    # Convert to DataFrames
    tracks_df = pd.DataFrame(track_data)
    artists_df = pd.DataFrame(artist_data)

    # Remove duplicates in artists_df
    artists_df = artists_df.drop_duplicates(subset=['artist_id', 'track_id', 'artist_name']).reset_index(drop=True)

    # Check if the DataFrame is empty before accessing columns
    if not tracks_df.empty:
        # Handle mixed date formats in 'album_release_date'
        def parse_album_date(date_str):
            if len(date_str) == 4:  # Year only
                return pd.to_datetime(date_str, format="%Y", errors='coerce')
            elif len(date_str) == 7:  # Year and month only
                return pd.to_datetime(date_str, format="%Y-%m", errors='coerce')
            else:  # Full date
                return pd.to_datetime(date_str, format="%Y-%m-%d", errors='coerce')

        tracks_df['album_release_date'] = tracks_df['album_release_date'].apply(parse_album_date)

        # Format for filename
        date_str = datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
        tracks_file_name = f'tracks_{date_str}.csv'
        artists_file_name = f'artists_{date_str}.csv'

        # Save DataFrames to S3
        save_to_s3(tracks_df, S3_BUCKET, S3_TRACKS_FOLDER, tracks_file_name)
        save_to_s3(artists_df, S3_BUCKET, S3_ARTISTS_FOLDER, artists_file_name)
    else:
        print("No tracks found for today.")
