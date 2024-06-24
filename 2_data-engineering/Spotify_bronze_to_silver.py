import pandas as pd
import os

# Paths
data_folder = os.path.join(os.path.dirname(__file__), '..', '0_data-bronze')
input_file_path  = os.path.join(data_folder, 'ingested_Spotify_5.csv')

silver_folder = os.path.join(os.path.dirname(__file__), '..', '0-data-silver')
output_file_path = os.path.join(silver_folder, 'Spotify.csv')

# Read the CSV file
df = pd.read_csv(input_file_path)

# Ensure the 'date' column is in datetime format
df['date'] = pd.to_datetime(df['date'])

# Create a new column for the end of week date set to Friday
df['End of Week Date'] = df['date'] + pd.offsets.Week(weekday=4)  # Set to the last day (Friday) of the week

# Group by 'End of Week Date' and calculate SWAV
weekly_swav = df.groupby('End of Week Date').apply(lambda x: pd.Series({
    'SWAV': (x['streams'] * x['Valence']).sum() / x['streams'].sum()
})).reset_index()

# Calculate Change in SWAV compared to the previous week
weekly_swav['Change in SWAV'] = weekly_swav['SWAV'].diff()

# Keep only relevant Columns
weekly_swav = weekly_swav[['End of Week Date', 'Change in SWAV']]

# Save the result to a new CSV file
weekly_swav.to_csv(output_file_path, index=False)

print(f"Weekly SWAV CSV saved to: {output_file_path}")