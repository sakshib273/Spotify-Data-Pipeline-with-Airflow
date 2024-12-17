import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

config = configparser.ConfigParser()
config.read('/home/sakshi/spotify_project/config.ini')

CLIENT_ID = config['spotify']['client_id']
CLIENT_SECRET = config['spotify']['client_secret']

sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id=CLIENT_ID, client_secret=CLIENT_SECRET))

data = pd.read_csv('/home/sakshi/spotify_project/cover_urls/track_url.csv')  
cover_urls = []

for url in data['track_url']:
    try:
        
        track_id = url.split('/')[-1].split('?')[0]
        
        
        track_info = sp.track(track_id)
        cover_url = track_info['album']['images'][0]['url'] 
        cover_urls.append(cover_url)
    except Exception as e:
        print(f"Error processing URL {url}: {e}")
        cover_urls.append(None)


data['cover_url'] = cover_urls

data.to_csv('/home/sakshi/spotify_project/cover_urls/tracks_with_covers.csv', index=False)
print("Cover URLs have been added and saved to tracks_with_covers.csv")