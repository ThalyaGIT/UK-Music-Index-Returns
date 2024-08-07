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

# First , get the DCC!
# Calculate the daily average cloud cover
data = filtered_data.groupby('Date', as_index=False)['Cloud_Cover'].mean()

# Calculate the Rolling-7-days-Average-Cloud-Cover
data['7D_Rolling_Avg_Cloud_Cover'] = data['Cloud_Cover'].rolling(window=7, min_periods=1).mean()

# Deseasonalize the cloud cover by substracting Rolling-7-days-Average-Cloud-Cover
data['DCC'] = data['Cloud_Cover'] - data['7D_Rolling_Avg_Cloud_Cover'] 

# Next, get the average daily change in deseasonalized cloud cover within a week
# Calculate daily changes in DCC  (Today minus yesteday's DCC)
data['Previous_Day_DCC'] = data['DCC'].shift(1)
data['Change_in_DCC'] = data['DCC']- data['Previous_Day_DCC']

# Calculate the average change in DCC for the past seven dats
data['7D_Rolling_Avg_Change_in_DCC'] = data['Change_in_DCC'].rolling(window=7, min_periods=1).mean()

# Select relevant columns to save
#final_data = data[['Date','Cloud_Cover','7D_Rolling_Avg_Cloud_Cover','DCC','Previous_Day_DCC','Change_in_DCC', '7D_Rolling_Avg_Change_in_DCC']].drop_duplicates()

final_data = data[['Date','Cloud_Cover','7D_Rolling_Avg_Cloud_Cover','DCC', 'Previous_Day_DCC','Change_in_DCC', '7D_Rolling_Avg_Change_in_DCC']].drop_duplicates()

# Define the path to save the new CSV file in the "silver" folder
silver_folder = os.path.join(os.path.dirname(__file__), '..', '0-data-silver')
output_file = os.path.join(silver_folder, 'Cloud.csv')

# Save the new DataFrame to a CSV file in the "silver" folder
final_data.to_csv(output_file, index=False)

# Display message
print('Deseasonalized Cloud Cover (DCC) data processed and saved to the silver layer.')

