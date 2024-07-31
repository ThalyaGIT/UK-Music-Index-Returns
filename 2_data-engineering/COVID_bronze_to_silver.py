import pandas as pd
import os

# Define the path to the CSV file
data_folder = os.path.join(os.path.dirname(__file__), '..', '0_data-bronze')
csv_file = os.path.join(data_folder, 'downloaded_COVID.csv')

# Load the CSV file into a pandas DataFrame
df = pd.read_csv(csv_file)

# Convert 'Date' column to datetime format
df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d')

# Filter on data
df = df[df['CountryCode'] == 'GBR']

# Keep Columns you need
df = df[['Date', 'StringencyIndex_Average']]

# Sort the DataFrame by date if it's not already sorted
df = df.sort_values(by='Date')

# Set the date column as the index
df.set_index('Date', inplace=True)

# Shift the 'ADS Index' column to get the lagged data
df['Previous Stringency'] = df['StringencyIndex_Average'].shift(7)

# Calculate the ADS_Change from the previous week's closing
df['Stringency_Change'] = df['StringencyIndex_Average'] - df['Previous Stringency']

# Drop rows with NaN values (the first 7 rows where lagged data is not available)
df.dropna(inplace=True)

# Reset the index to have a clean DataFrame
df.reset_index(inplace=True)

# Define the path to save the new CSV file in the "silver" folder
silver_folder = os.path.join(os.path.dirname(__file__), '..', '0-data-silver')
output_file = os.path.join(silver_folder, 'COVID.csv')

# Save the new DataFrame to a CSV file in the "silver" folder
df.to_csv(output_file, index=False)

# Display the new DataFrame
print(df)