import pandas as pd
import os

shift = 15
# shift = 7


# Input Path and CSV File
data_folder = os.path.join(os.path.dirname(__file__), '..', '0_data-bronze')
input_file_path = os.path.join(data_folder, 'ingested_Spotify_5.csv')

# Output Path and CSV File
silver_folder = os.path.join(os.path.dirname(__file__), '..', '0-data-silver')
output_file_path = os.path.join(silver_folder, 'Spotify.csv')

# Read the Input CSV file
df = pd.read_csv(input_file_path)

# Ensure the date column is in datetime format
df['Date'] = pd.to_datetime(df['date'])

# Sort the DataFrame by date if it's not already sorted
df = df.sort_values(by='Date')

# Set the date column as the index
df.set_index('Date', inplace=True)

# Group by date and calculate SWAV
df = df.groupby('Date').apply(lambda x: (x['streams'] * x['Valence']).sum() / x['streams'].sum()).reset_index(name='SWAV')

# Ensure 'Date' is a datetime column
df['Date'] = pd.to_datetime(df['Date'])

# Set the date column as the index again
df.set_index('Date', inplace=True)

# Shift the 'Price' column to get the lagged data
df['SWAV_lagged'] = df['SWAV'].shift(shift)

# Calculate Change in SWAV
df['Change in SWAV'] = df['SWAV'] - df['SWAV_lagged']

# Select only relevant columns
result_df = df[['SWAV', 'SWAV_lagged', 'Change in SWAV']].reset_index()

# Save to the output CSV and path.
result_df.to_csv(output_file_path, index=False)

# Print Success message
print(f"Spotify Silver CSV saved successfully")