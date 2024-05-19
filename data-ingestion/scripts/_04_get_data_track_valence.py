import os
import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

def add_valence_column(input_file_path, client_id, client_secret):
    
    current_directory = os.path.dirname(os.path.abspath(__file__)) #refactor directory stuff 

    output_file_path = os.path.join(os.path.dirname(current_directory), 'data', 'Spotify', 'spotify_tracks_with_valence.csv')

    """
    Function to add a Valence column to a CSV file containing track IDs.
    """
    def get_spotify_client(client_id, client_secret):
        """
        Function to get the Spotify API client.
        """
        try:
            if client_id and client_secret:
                # Initialize Spotipy client
                return spotipy.Spotify(client_credentials_manager=SpotifyClientCredentials(client_id=client_id,
                                                                                            client_secret=client_secret,
                                                                                            requests_timeout=10))
            else:
                print("Please provide Spotify API credentials.")
                return None
        except Exception as e:
            print(f"Error initializing Spotify client: {e}")
            return None

    def get_valence(track_id, spotify_client):
        """
        Function to retrieve valence feature for a given track ID from the Spotify API.
        """
        try:
            # Get track features from Spotify API
            track_features = spotify_client.audio_features(track_id)
            if track_features:
                return track_features[0]['valence']
            else:
                return None
        except Exception as e:
            print(f"Error retrieving valence for track {track_id}: {e}")
            return None

    def add_valence_column_to_df(df, spotify_client):
        """
        Function to add a Valence column to a DataFrame containing track IDs.
        """
        if spotify_client:
            # Add Valence column to DataFrame
            df['Valence'] = df['track_id'].apply(lambda x: get_valence(x, spotify_client))
            return df
        else:
            return None

    # Get Spotify API client
    spotify_client = get_spotify_client(client_id, client_secret)

    # Check if Spotify client is available
    if spotify_client:
        # Read CSV file into DataFrame
        df = pd.read_csv(input_file_path, nrows=5) # refactor

        # Add Valence column to DataFrame
        df_with_valence = add_valence_column_to_df(df, spotify_client)

        if df_with_valence is not None:
            # Save DataFrame with Valence column to CSV file
            df_with_valence.to_csv(output_file_path, index=False)
            print(f'Valence features have been added to {output_file_path}')
        else:
            print("Unable to add Valence column. Exiting function.")
    else:
        print("Spotify client not available. Exiting function.")

    return output_file_path
