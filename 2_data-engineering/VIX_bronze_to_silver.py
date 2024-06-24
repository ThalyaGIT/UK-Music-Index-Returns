import pandas as pd
import os

# Define the path to the CSV file
data_folder = os.path.join(os.path.dirname(__file__), '..', '0_data-bronze')
csv_file = os.path.join(data_folder, 'downloaded_VIX.csv')

# Load the CSV file into a pandas DataFrame
df = pd.read_csv(csv_file)

# Ensure the date column is in datetime format
df['Date'] = pd.to_datetime(df['Date'])

# Set the date column as the index
df.set_index('Date', inplace=True)

# Resample the data to get the last closing price of each week (last trading day of each week)
weekly_df = df['Close'].resample('W-FRI').last()

# Create a new DataFrame to store the required columns
result_df = pd.DataFrame()
result_df['End of Week Date'] = weekly_df.index
result_df['Vix Close'] = weekly_df.values

# Keep only relevant Columns
result_df = result_df[['End of Week Date', 'Vix Close']]


# Drop the first row since it won't have a 'Previous End of Week Closing'
result_df.dropna(inplace=True)

# Reset the index to have a clean DataFrame
result_df.reset_index(drop=True, inplace=True)

# Define the path to save the new CSV file in the "silver" folder
silver_folder = os.path.join(os.path.dirname(__file__), '..', '0-data-silver')
output_file = os.path.join(silver_folder, 'VIX.csv')

# Save the new DataFrame to a CSV file in the "silver" folder
result_df.to_csv(output_file, index=False)

# Display the new DataFrame
print(result_df)

