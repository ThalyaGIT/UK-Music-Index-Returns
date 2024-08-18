import pandas as pd
import os
import sys

def main(days, bronze_data_folder, silver_data_folder):   

    days = int(days)

    # Input Path and CSV File
    input_file_path = os.path.join(bronze_data_folder, 'ingested_Spotify_5.csv')

    # Output Path and CSV File
    output_file_path = os.path.join(silver_data_folder, 'Spotify.csv')

    # Read the Input CSV file
    df = pd.read_csv(input_file_path)

    # Ensure the date column is in datetime format
    df['Date'] = pd.to_datetime(df['date'])

    # Filter to only include the top 50 ranks
    df_top50 = df[df['rank'] >= 50]

    # Sort the DataFrame by date if it's not already sorted
    df_top50 = df_top50.sort_values(by='Date')

    # Set the date column as the index
    df_top50.set_index('Date', inplace=True)

    # Group by date and calculate SWAV
    df_swav = df_top50.groupby('Date').apply(lambda x: (x['streams'] * x['Valence']).sum() / x['streams'].sum()).reset_index(name='SWAV')

    # Ensure 'Date' is a datetime column
    df_swav['Date'] = pd.to_datetime(df_swav['Date'])

    # Set the date column as the index again
    df_swav.set_index('Date', inplace=True)

    # Shift the 'SWAV' column to get the lagged data
    df_swav['SWAV_lagged'] = df_swav['SWAV'].shift(days)

    # Calculate Change in SWAV
    df_swav['Change in SWAV'] = df_swav['SWAV'] - df_swav['SWAV_lagged']

    # Select only relevant columns
    result_df = df_swav[['SWAV', 'SWAV_lagged', 'Change in SWAV']].reset_index()

    # Save to the output CSV and path.
    result_df.to_csv(output_file_path, index=False)

    # Print Success message
    print(f"___Spotify data processed and saved to silver layer")
    
    
if __name__ == "__main__":
    if len(sys.argv) > 3:
        param1 = sys.argv[1]
        bronze_data_folder = sys.argv[3]
        silver_data_folder = sys.argv[4]
        main(param1, bronze_data_folder , silver_data_folder)
    else:
        print("No parameters provided.")