import pandas as pd
import os
from io import StringIO

# Define the path to the CSV file
data_folder = os.path.join(os.path.dirname(__file__), '..', '0_data-bronze')
csv_file = os.path.join(data_folder, 'downloaded_FTSE250.csv')

# Step 1: Read the CSV file content as a string
with open(csv_file, 'r', encoding='utf-8-sig') as file:
    file_content = file.read()

# Step 2: Replace all double quotes in the content
cleaned_content = file_content.replace('"', '')

# Step 3: Load the cleaned content into a pandas DataFrame
df = pd.read_csv(StringIO(cleaned_content))

print(df.head())

# Ensure the date column is in datetime format
df['date'] = pd.to_datetime(df['Date'])

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
result_df['% FTSE250 Change'] = ((result_df['This Week\'s End of Week Closing'] - result_df['Previous End of Week Closing']) / result_df['Previous End of Week Closing']) * 100

# Round up % FTSE100 Change to 2 decimal places
result_df['% FTSE250 Change'] = result_df['% FTSE250 Change'].apply(lambda x: round(x, 2))

# Calculate the previous week's % FTSE100 Change
result_df['Previous Week % FTSE250 Change'] = result_df['% FTSE250 Change'].shift(1)

# Keep only relevant Columns
result_df = result_df[['End of Week Date', '% FTSE250 Change']]


# Drop the first row since it won't have a 'Previous End of Week Closing'
result_df.dropna(inplace=True)

# Reset the index to have a clean DataFrame
result_df.reset_index(drop=True, inplace=True)

# Define the path to save the new CSV file in the "silver" folder
silver_folder = os.path.join(os.path.dirname(__file__), '..', '0-data-silver')
output_file = os.path.join(silver_folder, 'FTSE250.csv')

# Save the new DataFrame to a CSV file in the "silver" folder
result_df.to_csv(output_file, index=False)

