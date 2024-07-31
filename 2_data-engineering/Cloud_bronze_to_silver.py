import pandas as pd
import os

# Define the path to the CSV file
data_folder = os.path.join(os.path.dirname(__file__), '..', '0_data-bronze')
csv_file = os.path.join(data_folder, 'downloaded_Cloud.csv')

# Load your cloud cover data
data  = pd.read_csv(csv_file)

# Convert 'Date' to datetime format and 'Time' to string format
data['Date'] = pd.to_datetime(data['Date'], format='%d/%m/%Y')
data['Time'] = data['Time'].astype(str)

# Filter the data to include only times between 08:00 and 17:00
filtered_data = data[(data['Time'] >= '08:00:00') & (data['Time'] <= '17:00:00')].copy()

# Calculate daily average cloud cover
daily_avg = filtered_data.groupby('Date', as_index=False)['Cloud_Cover'].mean()

# Calculate the 7-day rolling average cloud cover
daily_avg['7D_Rolling_Avg'] = daily_avg['Cloud_Cover'].rolling(window=7, min_periods=1).mean()

# Calculate the overall average cloud cover
overall_avg = daily_avg['Cloud_Cover'].mean()

# Deseasonalize the 7-day rolling average by subtracting the overall average
daily_avg['DCC'] = daily_avg['7D_Rolling_Avg'] - overall_avg

# Merge the deseasonalized data back with the original filtered data
final_data = pd.merge(filtered_data, daily_avg[['Date', 'DCC', '7D_Rolling_Avg']], on='Date', how='left')

# Select relevant columns to save
final_data = final_data[['Date', '7D_Rolling_Avg', 'DCC']].drop_duplicates()

# Define the path to save the new CSV file in the "silver" folder
silver_folder = os.path.join(os.path.dirname(__file__), '..', '0-data-silver')
output_file = os.path.join(silver_folder, 'Cloud.csv')

# Save the new DataFrame to a CSV file in the "silver" folder
final_data.to_csv(output_file, index=False)


