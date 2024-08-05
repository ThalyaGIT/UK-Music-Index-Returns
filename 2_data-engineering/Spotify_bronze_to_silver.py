import pandas as pd
import os

# Input Path and CSV File
data_folder = os.path.join(os.path.dirname(__file__), '..', '0_data-bronze')
input_file_path = os.path.join(data_folder, 'ingested_Spotify_5.csv')

# Output Path and CSV File
silver_folder = os.path.join(os.path.dirname(__file__), '..', '0-data-silver')
output_file_path = os.path.join(silver_folder, 'Spotify.csv')

# Read the Input CSV file
df = pd.read_csv(input_file_path)

# Ensure the 'date' column is in datetime format
df['Date'] = pd.to_datetime(df['date'])

# Group by date and calculate SWAV
df = df.groupby('Date').apply(lambda x: (x['streams'] * x['Valence']).sum() / x['streams'].sum()).reset_index(name='SWAV')

# For each date, add a new column containing the SWAV value from 7 days earlier
df['SWAV_lagged'] = df['SWAV'].shift(7)

# Calculate Change in SWAV
df['Change in SWAV'] = df['SWAV'] - df['SWAV_lagged']

# One-week-lagged-Change-in-SWAV
df['Change_in_SWAV_lagged'] = df['Change in SWAV'].shift(7)

# Select only relevant columns
result_df = df[['Date', 'SWAV', 'SWAV_lagged', 'Change in SWAV', 'Change_in_SWAV_lagged']].drop_duplicates(subset=['Date'], keep='last')

# Save to the output CSV and path.
result_df.to_csv(output_file_path, index=False)

# Print Success message
print(f"Spotify Silver CSV saved successfully")