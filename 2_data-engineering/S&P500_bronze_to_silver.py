import pandas as pd
import os

# Define the path to the CSV file
data_folder = os.path.join(os.path.dirname(__file__), '..', '0_data-bronze')
csv_file = os.path.join(data_folder, 'downloaded_S&P500.csv')

# Load the CSV file into a pandas DataFrame
df = pd.read_csv(csv_file)

# Ensure the date column is in datetime format
df['Date'] = pd.to_datetime(df['Date'])

# Sort the DataFrame by date if it's not already sorted
df = df.sort_values(by='Date')

# Set the date column as the index
df.set_index('Date', inplace=True)

# Shift the 'Price' column to get the lagged data
df['Previous Week Price'] = df[' Close'].shift(7)

# Calculate the percentage change from the previous week's closing
df['% S&P500 Change'] = ((df[' Close'] - df['Previous Week Price']) / df['Previous Week Price']) * 100

# Round up % S&P500 Change to 2 decimal places
df['% S&P500 Change'] = df['% S&P500 Change'].round(2)

df['Previous Week % S&P500 Change'] = df['% S&P500 Change'].shift(7)

# Keep only relevant columns
result_df = df[[' Close', 'Previous Week % S&P500 Change', '% S&P500 Change']]

# Drop rows with NaN values (first 7 rows will have NaN for 'Previous Week Price')
result_df.dropna(inplace=True)

# Reset the index to have a clean DataFrame
result_df.reset_index(inplace=True)

# Define the path to save the new CSV file in the "silver" folder
silver_folder = os.path.join(os.path.dirname(__file__), '..', '0-data-silver')
output_file = os.path.join(silver_folder, 'S&P500.csv')

# Save the new DataFrame to a CSV file in the "silver" folder
result_df.to_csv(output_file, index=False)

# Display the new DataFrame
print(result_df)