import os
import pandas as pd

def process_data_add_track_id(input_file_path):

    # Get the current working directory
    current_directory = os.path.dirname(os.path.abspath(__file__))

    # Read the CSV file into a DataFrame
    df = pd.read_csv(input_file_path)

    # Extract track ID from 'uri' column
    df['track_id'] = df['uri'].str.split(':').str[-1]

      # Define the output file path
    output_file_path = os.path.join(os.path.dirname(current_directory), 'data', 'Spotify', '02_processed_data_added_track_id.csv')

    # Save the combined dataframe to a CSV file
    df.to_csv(output_file_path, index=False)

    print(f'Spotify Track Ids have been extracted for all tracks and stored into {output_file_path}')

    return output_file_path