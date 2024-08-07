import pandas as pd
import os

# Define the path to the CSV file
data_folder = os.path.join(os.path.dirname(__file__), '..', '0_data-bronze')
csv_file = os.path.join(data_folder, 'downloaded_COVID.csv')

# Load the CSV file into a pandas DataFrame
df = pd.read_csv(csv_file)

# Convert 'Date' column to datetime format
df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d')

# Filter on data for GB
df = df[df['CountryCode'] == 'GBR']

# Filter on data for all regions
df = df[df['RegionCode'].isna()]

# Keep Columns you need
df = df[['Date'
         , 'C1M_School closing'
         , 'C2M_Workplace closing'
         , 'C3M_Cancel public events'
         , 'C4M_Restrictions on gatherings'
         , 'C5M_Close public transport'
         , 'C6M_Stay at home requirements'
         ]]

df['Covid_Stringency'] = df.iloc[:, 1:].sum(axis=1)


# Sort the DataFrame by date if it's not already sorted
df = df.sort_values(by='Date')

# Set the date column as the index
df.set_index('Date', inplace=True)

# Shift the 'ADS Index' column to get the lagged data
df['Last_Week_Covid_Stringency'] = df['Covid_Stringency'].shift(7)

# Calculate the ADS_Change from the previous week's closing
df['Stringency_Change'] = df['Covid_Stringency'] - df['Last_Week_Covid_Stringency']

# Calculate the ADS_Change from the previous week's closing
df = df[['Stringency_Change'
         , 'Covid_Stringency'
         , 'Last_Week_Covid_Stringency'
         ]]



# Drop rows with NaN values (the first 7 rows where lagged data is not available)
df.dropna(inplace=True)

# Reset the index to have a clean DataFrame
df.reset_index(inplace=True)

# Define the path to save the new CSV file in the "silver" folder
silver_folder = os.path.join(os.path.dirname(__file__), '..', '0-data-silver')
output_file = os.path.join(silver_folder, 'COVID.csv')

# Save the new DataFrame to a CSV file in the "silver" folder
df.to_csv(output_file, index=False)

# Display message
print('Covid Stringency data processed and saved to silver layer')