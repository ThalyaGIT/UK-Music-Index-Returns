import pandas as pd
import os

shift = 10
# shift = 7

# Define the path to the CSV file
data_folder = os.path.join(os.path.dirname(__file__), '..', '0_data-bronze')
csv_file = os.path.join(data_folder, 'downloaded_MSCI.csv')

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

# Sort the DataFrame by date if it's not already sorted
df = df.sort_values(by='Date')

# Set the date column as the index
df.set_index('Date', inplace=True)


# Shift the 'Price' column to get the lagged data
df['MSCI Previous Week Price'] = df['Price'].shift(shift)

# Calculate the percentage change from the previous week's closing
df['% MSCI Change'] = ((df['Price'] - df['MSCI Previous Week Price']) / df['MSCI Previous Week Price']) * 100

# Round up % FTSE100 Change to 2 decimal places
df['% MSCI Change'] = df['% MSCI Change'].round(2)

df['Previous Week % MSCI Change'] = df['% MSCI Change'].shift(shift)

# Keep only relevant columns
result_df = df[['Price', 'MSCI Previous Week Price', 'Previous Week % MSCI Change', '% MSCI Change']]

# Drop rows with NaN values (first 7 rows will have NaN for 'Previous Week Price')
result_df.dropna(inplace=True)

# Reset the index to have a clean DataFrame
result_df.reset_index(inplace=True)

# Define the path to save the new CSV file in the "silver" folder
silver_folder = os.path.join(os.path.dirname(__file__), '..', '0-data-silver')
output_file = os.path.join(silver_folder, 'MSCI.csv')

# Save the new DataFrame to a CSV file in the "silver" folder
result_df.to_csv(output_file, index=False)

# Display message
print('MSCI data processed and saved to silver layer')