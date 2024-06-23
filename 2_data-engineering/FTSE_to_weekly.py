import pandas as pd
import os

# Define the path to the CSV file
data_folder = os.path.join(os.path.dirname(__file__), '..', '0_data-downloaded')
csv_file = os.path.join(data_folder, 'ftse100_data.csv')

# Load the CSV file into a pandas DataFrame
df = pd.read_csv(csv_file)

# Ensure the date column is in datetime format
df['date'] = pd.to_datetime(df['date'])

# Set the date column as the index
df.set_index('date', inplace=True)

# Resample the data to get the last closing price of each week (Friday's closing price)
weekly_df = df['Price'].resample('W-FRI').last()

# Create a new DataFrame to store the required columns
result_df = pd.DataFrame()
result_df['End of Week Date'] = weekly_df.index
result_df['This Week\'s End of Week Closing'] = weekly_df.values

# Shift the 'This Week's End of Week Closing' column to get the 'Previous End of Week Closing'
result_df['Previous End of Week Closing'] = result_df['This Week\'s End of Week Closing'].shift(1)

# Drop the first row since it won't have a 'Previous End of Week Closing'
result_df.dropna(inplace=True)

# Reset the index to have a clean DataFrame
result_df.reset_index(drop=True, inplace=True)

# Display the new DataFrame
print(result_df)

