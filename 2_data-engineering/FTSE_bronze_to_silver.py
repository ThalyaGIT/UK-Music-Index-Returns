import pandas as pd
import os

# Define the path to the CSV file
data_folder = os.path.join(os.path.dirname(__file__), '..', '0_data-bronze')
csv_file = os.path.join(data_folder, 'downloaded_FTSE100.csv')

# Load the CSV file into a pandas DataFrame
df = pd.read_csv(csv_file)

# Ensure the date column is in datetime format
df['Date'] = pd.to_datetime(df['date'])

# Sort the DataFrame by date if it's not already sorted
df = df.sort_values(by='Date')

# Set the date column as the index
df.set_index('Date', inplace=True)

# Shift the 'Price' column to get the lagged data
df['Previous Week Price'] = df['Price'].shift(7)

# Calculate the percentage change from the previous week's closing
df['% FTSE100 Change'] = ((df['Price'] - df['Previous Week Price']) / df['Previous Week Price']) * 100

# Round up % FTSE100 Change to 2 decimal places
df['% FTSE100 Change'] = df['% FTSE100 Change'].round(2)

df['Previous Week % FTSE100 Change'] = df['% FTSE100 Change'].shift(7)

df['Next Week % FTSE100 Change'] = df['% FTSE100 Change'].shift(-7)

# Keep only relevant columns
result_df = df[['Price', 'Previous Week % FTSE100 Change', '% FTSE100 Change','Next Week % FTSE100 Change']]

# Drop rows with NaN values (first 7 rows will have NaN for 'Previous Week Price')
result_df.dropna(inplace=True)

# Reset the index to have a clean DataFrame
result_df.reset_index(inplace=True)

# Define the path to save the new CSV file in the "silver" folder
silver_folder = os.path.join(os.path.dirname(__file__), '..', '0-data-silver')
output_file = os.path.join(silver_folder, 'FTSE100.csv')

# Save the new DataFrame to a CSV file in the "silver" folder
result_df.to_csv(output_file, index=False)

# Display message
print('FTSE 100 data processed and saved to silver layer')