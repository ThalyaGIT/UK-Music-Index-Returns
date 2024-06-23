import pandas as pd
import os

# Define the path to the CSV file
data_folder = os.path.join(os.path.dirname(__file__), '..', '0_data-bronze')
csv_file = os.path.join(data_folder, 'downloaded_FTSE100.csv')

# Load the CSV file into a pandas DataFrame
df = pd.read_csv(csv_file)

# Ensure the date column is in datetime format
df['date'] = pd.to_datetime(df['date'])

# Set the date column as the index
df.set_index('date', inplace=True)

# Resample the data to get the last closing price of each week (last trading day of each week)
weekly_df = df['Price'].resample('W-FRI').last()

# Create a new DataFrame to store the required columns
result_df = pd.DataFrame()
result_df['End of Week Date'] = weekly_df.index
result_df['This Week\'s End of Week Closing'] = weekly_df.values

# Shift the 'This Week's End of Week Closing' column to get the 'Previous End of Week Closing'
result_df['Previous End of Week Closing'] = result_df['This Week\'s End of Week Closing'].shift(1)

# Calculate the percentage change from the previous week's closing
result_df['% Change'] = ((result_df['This Week\'s End of Week Closing'] - result_df['Previous End of Week Closing']) / result_df['Previous End of Week Closing']) * 100

# Round up % Change to 2 decimal places
result_df['% Change'] = result_df['% Change'].apply(lambda x: round(x, 2))

# Calculate the previous week's % Change
result_df['Previous Week % Change'] = result_df['% Change'].shift(1)

# Keep only relevant Columns
result_df = result_df[['End of Week Date', '% Change', 'Previous Week % Change']]


# Drop the first row since it won't have a 'Previous End of Week Closing'
result_df.dropna(inplace=True)

# Reset the index to have a clean DataFrame
result_df.reset_index(drop=True, inplace=True)

# Define the path to save the new CSV file in the "silver" folder
silver_folder = os.path.join(os.path.dirname(__file__), '..', '0-data-silver')
output_file = os.path.join(silver_folder, 'FTSE100.csv')

# Save the new DataFrame to a CSV file in the "silver" folder
result_df.to_csv(output_file, index=False)

# Display the new DataFrame
print(result_df)

