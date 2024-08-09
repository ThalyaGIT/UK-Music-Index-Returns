import pandas as pd
import os

# Define the path to the CSV file
data_folder = os.path.join(os.path.dirname(__file__), '..', '0_data-bronze')
csv_file = os.path.join(data_folder, 'downloaded_VIX.csv')

# prepare dates that exist in ftse 
ftse_file = os.path.join(data_folder, 'downloaded_FTSE100.csv')
ftse = pd.read_csv(ftse_file)
ftse['Date'] = pd.to_datetime(ftse['date'])
ftse.set_index('Date', inplace=True)

# Load the CSV file into a pandas DataFrame
df = pd.read_csv(csv_file)

# Ensure the date column is in datetime format
df['Date'] = pd.to_datetime(df['Date'])

# Filter df to only include rows where the 'Date' is in ftse's index
df = df[df['Date'].isin(ftse.index)]

# Drop columns 'Open', 'High', and 'Low' if they exist
columns_to_drop = ['Open', 'High', 'Low']
df.drop(columns=columns_to_drop, inplace=True, errors='ignore')

df['Vix Close'] = df['Close'] 

# Define the path to save the new CSV file in the "silver" folder
silver_folder = os.path.join(os.path.dirname(__file__), '..', '0-data-silver')
output_file = os.path.join(silver_folder, 'VIX.csv')

# Save the modified DataFrame to a CSV file in the "silver" folder
df.to_csv(output_file, index=False)

# Display message
print('Volatility Index (VIX) data processed and saved to silver layer')