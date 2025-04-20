import os
import json
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import supybot.callbacks as callbacks
from supybot.commands import *

class Spotify(callbacks.Plugin):
    """Fetches the currently playing track from Spotify."""

    def __init__(self, irc):
        self.__parent = super(Spotify, self)
        self.__parent.__init__(irc)

        # Load credentials from the saved JSON file
        self.credentials_path = os.path.join(os.path.dirname(__file__), 'spotify_credentials.json')
        if not os.path.exists(self.credentials_path):
            raise ValueError("Spotify credentials file not found!")

        with open(self.credentials_path, 'r') as f:
            self.credentials = json.load(f)

        self.client_id = self.credentials['client_id']
        self.client_secret = self.credentials['client_secret']
        self.refresh_token = self.credentials['refresh_token']

        # Define a custom cache path for the token
        self.cache_path = os.path.join(os.path.dirname(__file__), '.spotify_cache')

        # Initialize SpotifyOAuth with custom cache path
        self.auth_manager = SpotifyOAuth(
            client_id=self.client_id,
            client_secret=self.client_secret,
            redirect_uri='http://127.0.0.1:8080',
            scope='user-read-currently-playing',
            cache_path=self.cache_path,  # Specify custom cache path
            open_browser=False
        )

        # Refresh the access token using the refresh token
        token_info = self.auth_manager.refresh_access_token(self.refresh_token)
        self.access_token = token_info['access_token']

        # Initialize Spotify client with the access token and auth_manager
        self.sp = spotipy.Spotify(auth=self.access_token, auth_manager=self.auth_manager)

    def _refresh_access_token(self):
        """Refresh the Spotify access token."""
        token_info = self.auth_manager.refresh_access_token(self.refresh_token)
        self.access_token = token_info['access_token']
        self.sp = spotipy.Spotify(auth=self.access_token, auth_manager=self.auth_manager)

    def ms_to_minutes_seconds(self, ms):
        """Convert milliseconds to minutes:seconds format."""
        total_seconds = ms / 1000
        minutes = total_seconds // 60
        seconds = total_seconds % 60
        return f"{int(minutes)}:{int(seconds):02}"

    def np(self, irc, msg, args):
        """Displays the current track being played on Spotify."""
        try:
            current_track = self.sp.current_playback()
            if not current_track or not current_track['is_playing']:
                irc.reply("No track is currently playing.")
                return

            # Track information
            track_name = current_track['item']['name']
            artists = ', '.join(artist['name'] for artist in current_track['item']['artists'])
            album = current_track['item']['album']['name']
            url = current_track['item']['external_urls']['spotify']

            # Duration and current position
            duration_ms = current_track['item']['duration_ms']
            current_position_ms = current_track['progress_ms']
            current_position = self.ms_to_minutes_seconds(current_position_ms)
            total_duration = self.ms_to_minutes_seconds(duration_ms)

            message = f"Now playing: {artists} - {track_name} | Album: {album} | Progress: {current_position}/{total_duration} | Listen: {url}"

            # Ensure the message is sent as a single line
            #irc.reply(message, noEscape=True)
            irc.reply(message)

        except spotipy.exceptions.SpotifyException as e:
            if 'token expired' in str(e).lower():
                self._refresh_access_token()
                self.np(irc, msg, args)  # Retry after refreshing token
            else:
                irc.reply(f"An error occurred: {e}")

Class = Spotify