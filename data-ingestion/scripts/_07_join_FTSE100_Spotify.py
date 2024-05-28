import os
import pandas as pd

def join_ftse100_spotify(spotify_file, ftse100_file): # tech debt

    # Read the CSV files
    change_df = pd.read_csv(ftse100_file)  # Replace with the actual filename for date and change%
    valence_df = pd.read_csv(spotify_file)  # Replace with the actual filename for date and average valence

    # Select only the relevant columns from change_df
    change_df = change_df[['date', 'Change %']]

    # Ensure the dates are in the same format
    change_df['date'] = pd.to_datetime(change_df['date'])
    valence_df['date'] = pd.to_datetime(valence_df['date'])

    # Remove '%' from 'Change%' column and convert to numeric
    change_df['Change %'] = change_df['Change %'].str.replace('%', '').astype(float)

    # Merge the DataFrames on 'date'
    merged_df = pd.merge(change_df, valence_df, on='date')

    # Save the merged DataFrame to a new CSV file
    merged_df.to_csv('merged_data.csv', index=False)

    # Define the output file path
    output_file_path = os.path.join(os.path.dirname(spotify_file), '07_join_ftse100_spotify.csv')

    # Optionally, save the merged DataFrame to a new CSV file
    merged_df.to_csv(output_file_path, index=False)

    return output_file_path