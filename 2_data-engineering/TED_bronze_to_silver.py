import pandas as pd
import os

#shift = 5
shift = 7

# Define the path to the CSV file
data_folder = os.path.join(os.path.dirname(__file__), '..', '0_data-bronze')
csv_file = os.path.join(data_folder, 'downloaded_TED.csv')

# Load the CSV file into a pandas DataFrame
df = pd.read_csv(csv_file)

# Ensure the date column is in datetime format
df['Date'] = pd.to_datetime(df['Date'], format="%d/%m/%Y")

# Rename 'Value' column to 'TED'
df.rename(columns={'Value': 'TED'}, inplace=True)

# Sort the DataFrame by date if it's not already sorted
df = df.sort_values(by='Date')

# Set the date column as the index
df.set_index('Date', inplace=True)

# Drop rows with NaN values (first 7 rows will have NaN for 'Previous Week Price')
df.dropna(inplace=True)

# Reset the index to have a clean DataFrame
df.reset_index(inplace=True)

# Define the path to save the new CSV file in the "silver" folder
silver_folder = os.path.join(os.path.dirname(__file__), '..', '0-data-silver')
output_file = os.path.join(silver_folder, 'TED.csv')

# Save the new DataFrame to a CSV file in the "silver" folder
df.to_csv(output_file, index=False)

# Display message
print('TED data processed and saved to silver layer')