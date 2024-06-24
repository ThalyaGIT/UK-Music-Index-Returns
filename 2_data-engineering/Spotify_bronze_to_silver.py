import pandas as pd
import os

# Paths
data_folder = os.path.join(os.path.dirname(__file__), '..', '0_data-bronze')
input_file_path = os.path.join(data_folder, 'ingested_Spotify_5.csv')

silver_folder = os.path.join(os.path.dirname(__file__), '..', '0-data-silver')
output_file_path = os.path.join(silver_folder, 'Spotify.csv')

# Read the CSV file
df = pd.read_csv(input_file_path)

# Ensure the 'date' column is in datetime format
df['Date'] = pd.to_datetime(df['date'])

# Calculate SWAV for each day
df['SWAV'] = df['Valence'] * df['streams']

# Calculate lagged SWAV for each day (SWAV from seven days before)
df['SWAV_lagged'] = df.groupby('Date')['SWAV'].rolling(window=7, min_periods=1).sum().shift(7).reset_index(level=0, drop=True)

# Calculate Change in SWAV
df['Change in SWAV'] = df['SWAV'] - df['SWAV_lagged']

# Select relevant columns
result_df = df[['Date', 'SWAV', 'SWAV_lagged', 'Change in SWAV']].drop_duplicates(subset=['Date'], keep='last')

# Save to CSV
result_df.to_csv(output_file_path, index=False)

print(f"SWAV with lag and change CSV saved to: {output_file_path}")