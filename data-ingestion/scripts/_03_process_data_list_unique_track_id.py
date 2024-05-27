import os
import pandas as pd

def process_data_list_unique_track_id(input_file_path):
    """
    Function to extract unique track IDs from a CSV file.
    """
    # Read the CSV file into a DataFrame
    df = pd.read_csv(input_file_path)

    # Extract unique track IDs from the 'track_id' column
    unique_track_ids = df['track_id'].unique()

    # Create a DataFrame with the unique track IDs
    unique_track_ids_df = pd.DataFrame({'track_id': unique_track_ids})

    # Define the output file path
    output_file_path = os.path.join(os.path.dirname(input_file_path), '03_processed_data_list_unique_track_id.csv')

    # Save the unique track IDs to a CSV file
    unique_track_ids_df = unique_track_ids_df.head(500) ## remove later tech debt
    unique_track_ids_df.to_csv(output_file_path, index=False)

    print(f'Unique Spotify Track IDs have been stored in {output_file_path}')

    return output_file_path