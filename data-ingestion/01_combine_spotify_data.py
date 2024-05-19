import os
import pandas as pd


# Get the current working directory
current_directory = os.path.dirname(os.path.abspath(__file__))

# Define the directory containing the CSV files using an absolute path
directory = os.path.join(current_directory, 'data', 'raw', 'charts')

# List to hold dataframes
dataframes = []

# Loop through each file in the directory
for filename in os.listdir(directory):
    if filename.endswith('.csv'):
        filepath = os.path.join(directory, filename)
        df = pd.read_csv(filepath)
        dataframes.append(df)

# Concatenate all dataframes in the list
combined_df = pd.concat(dataframes, ignore_index=True)

# Define the output file path
output_file_path = os.path.join(current_directory, 'data','clean', 'combined_spotify_data.csv')

# Save the combined dataframe to a CSV file
combined_df.to_csv(output_file_path, index=False)

print(f'All CSV files have been combined into {output_file_path}')