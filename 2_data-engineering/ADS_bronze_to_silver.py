import pandas as pd
import os

# Define the path to the CSV file
data_folder = os.path.join(os.path.dirname(__file__), '..', '0_data-bronze')
csv_file = os.path.join(data_folder, 'downloaded_ADS.csv')

# Load the CSV file into a pandas DataFrame
df = pd.read_csv(csv_file)

# Ensure the date column is in datetime format
df['Date'] = pd.to_datetime(df['Date'])

# Sort the DataFrame by date if it's not already sorted
df = df.sort_values(by='Date')

# Set the date column as the index
df.set_index('Date', inplace=True)

# Shift the 'ADS Index' column to get the lagged data
df['Previous ADS Index'] = df['ADS Index'].shift(7)

# Calculate the ADS_Change from the previous week's closing
df['ADS_Change'] = df['ADS Index'] - df['Previous ADS Index']

# Keep only relevant columns
result_df = df[['ADS Index', 'Previous ADS Index', 'ADS_Change']]

# # Drop rows with NaN values (the first 7 rows where lagged data is not available)
# result_df.dropna(inplace=True)

# Reset the index to have a clean DataFrame
result_df.reset_index(inplace=True)

# Define the path to save the new CSV file in the "silver" folder
silver_folder = os.path.join(os.path.dirname(__file__), '..', '0-data-silver')
output_file = os.path.join(silver_folder, 'ADS.csv')

# Save the new DataFrame to a CSV file in the "silver" folder
result_df.to_csv(output_file, index=False)

# Display message
print('Aruoba-Diebold-Scotti Business Conditions (ADS) data processed and saved to silver layer')