from _01_combine_data_spotify_csvs import combine_data_spotify_csvs 
from _02_process_data_add_track_id import process_data_add_track_id
from _03_process_data_list_unique_track_id import process_data_list_unique_track_id 
from _04_get_data_track_valence import add_valence_column

def run_all_ingestion():
    # Prompt user to input Spotify client ID and client secret
    #spotify_client_id = input("Enter your Spotify client ID: ")
    #spotify_client_secret = input("Enter your Spotify client secret: ")
    
    # Combine Spotify CSV files
    combined_spotify_file = combine_data_spotify_csvs()
    
    # Process data and add track ID
    processed_data_with_track_id_file = process_data_add_track_id(combined_spotify_file)
    
    # Extract unique track IDs
    unique_track_id_file = process_data_list_unique_track_id(processed_data_with_track_id_file)
    
    # Add valence column
    #unique_tracks_with_valence = add_valence_column(unique_track_id_file, spotify_client_id, spotify_client_secret)

if __name__ == "__main__":
    run_all_ingestion()