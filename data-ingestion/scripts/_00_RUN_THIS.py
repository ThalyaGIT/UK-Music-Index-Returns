import os
from _01_combine_data_spotify_csvs import combine_data_spotify_csvs 
from _01_combine_data_spotify_csvs import compare_dates
from _02_process_data_add_track_id import process_data_add_track_id
from _03_process_data_list_unique_track_id import process_data_list_unique_track_id 
from _04_get_data_track_valence import add_valence_column

 # Get the current working directory
current_directory = os.path.dirname(os.path.abspath(__file__))

# Define the directory containing the CSV files using an absolute path
spotify_directory = os.path.join(os.path.dirname(current_directory), 'data', 'Spotify')
ftse100_directory = os.path.join(os.path.dirname(current_directory), 'data', 'FTSE100')

# Define the file name
spotify_file_name = '01_combined_data_spotify.csv'
ftse100_file_name = 'ftse100_data.csv'

# Construct the full file path
spotify_filepath = os.path.join(spotify_directory, spotify_file_name)
FTSE_100_filepath = os.path.join(ftse100_directory, ftse100_file_name)


def run_all_ingestion():
    # Prompt user to input Spotify client ID and client secret
    #spotify_client_id = input("Enter your Spotify client ID: ")
    #spotify_client_secret = input("Enter your Spotify client secret: ")
    
    # Combine Spotify CSV files
    combined_spotify_file = combine_data_spotify_csvs()

   # Validate that Spotify Dates and FTSE dates are complete
    compare_dates(combined_spotify_file, FTSE_100_filepath) ### Tech Debt 
    
    # Process data and add track ID
    processed_data_with_track_id_file = process_data_add_track_id(spotify_filepath)
    
    # Extract unique track IDs
    unique_track_id_file = process_data_list_unique_track_id(processed_data_with_track_id_file)
    
    # Add valence column
    #unique_tracks_with_valence = add_valence_column(unique_track_id_file, spotify_client_id, spotify_client_secret)

if __name__ == "__main__":
    run_all_ingestion()