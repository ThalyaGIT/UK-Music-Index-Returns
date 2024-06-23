import os
import pandas as pd

def get_avg_valence_by_date(input_file_path):

    # Read the CSV files
    df = pd.read_csv(input_file_path)  # Contains 'track_id' and 'valence'

   # Group by date and calculate the average valence
    grouped_df = df.groupby('date')['Valence'].mean().reset_index() # tech debt of valence

    # Optionally, rename the columns for clarity
    grouped_df.columns = ['date', 'average_valence']

    # Save the grouped DataFrame to a new CSV file
    grouped_df.to_csv('average_valence_by_date.csv', index=False)

    # Define the output file path
    output_file_path = os.path.join(os.path.dirname(input_file_path), '06_avg_valence_by_date.csv')

    # Optionally, save the merged DataFrame to a new CSV file
    grouped_df.to_csv(output_file_path, index=False)

    return output_file_path