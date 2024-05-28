import os
import pandas as pd

def merge_charts_with_valence(unique_tracks_filepath,all_tracks_filepath):

    # Read the CSV files
    unique_tracks_df = pd.read_csv(unique_tracks_filepath)  # Contains 'track_id' and 'valence'
    all_tracks_df = pd.read_csv(all_tracks_filepath)  # Contains 'track_id' and other columns

    # Merge the DataFrames on 'track_id'
    merged_df = pd.merge(all_tracks_df, unique_tracks_df, on='track_id', how='left')

    # Define the output file path
    output_file_path = os.path.join(os.path.dirname(all_tracks_filepath), '05_merged_charts_with_valence.csv')

    # Optionally, save the merged DataFrame to a new CSV file
    merged_df.to_csv(output_file_path, index=False)

    return output_file_path