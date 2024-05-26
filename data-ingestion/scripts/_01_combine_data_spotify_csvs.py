import os
import pandas as pd
import re

def combine_data_spotify_csvs():
    # Get the current working directory
    current_directory = os.path.dirname(os.path.abspath(__file__))

    # Define the directory containing the CSV files using an absolute path
    directory = os.path.join(os.path.dirname(current_directory), 'data', 'Spotify', 'csvs') # tech debts

    # List to hold dataframes
    dataframes = []

    # Loop through each file in the directory
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            filepath = os.path.join(directory, filename)
            # Extract date from filename using regular expression
            date = re.search(r'regional-gb-daily-(\d{4}-\d{2}-\d{2})', filename).group(1)
            # Read CSV file into dataframe
            df = pd.read_csv(filepath)
            # Add date column to dataframe
            df['date'] = date
            dataframes.append(df)

    # Concatenate all dataframes in the list
    combined_df = pd.concat(dataframes, ignore_index=True)

    # Define the output file path
    output_file_path = os.path.join(os.path.dirname(current_directory), 'data', 'Spotify', '01_combined_data_spotify.csv')

    # Save the combined dataframe to a CSV file
    combined_df.to_csv(output_file_path, index=False)

    print(f'All CSV files have been combined into {output_file_path}')

    return output_file_path

def compare_dates(file1, file2):
    # Read the CSV files
    df1 = pd.read_csv(file1)
    df2 = pd.read_csv(file2)
    
    # Ensure that the date columns are in datetime format with a specific format
    date_format = "%Y-%m-%d"  # Adjust this format based on your actual data
    df1['date'] = pd.to_datetime(df1['date'], format="%Y-%m-%d" ) # Spotify dates looks like this
    df2['date'] = pd.to_datetime(df2['date'], format="%d/%m/%Y" ) # FTSE dates look like this
    
    # Extract the unique dates from both files
    dates1 = set(df1['date'].unique())
    dates2 = set(df2['date'].unique())
    
    # Check if all dates in file2 are in file1
    missing_dates = dates2 - dates1
    
    if not missing_dates:
        return True, print("All dates in FTSE are present in Spotify.")
    else:
        return False, #print(f"The following dates are missing in Spotify: {missing_dates}")