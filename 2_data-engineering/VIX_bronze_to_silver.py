import pandas as pd
import os

# Define the path to the CSV file
data_folder = os.path.join(os.path.dirname(__file__), '..', '0_data-bronze')
csv_file = os.path.join(data_folder, 'downloaded_VIX.csv')

# Load the CSV file into a pandas DataFrame
df = pd.read_csv(csv_file)

# Drop columns 'Open', 'High', and 'Low' if they exist
columns_to_drop = ['Open', 'High', 'Low']
df.drop(columns=columns_to_drop, inplace=True, errors='ignore')

df['Vix Close'] = df['Close'] 

# Define the path to save the new CSV file in the "silver" folder
silver_folder = os.path.join(os.path.dirname(__file__), '..', '0-data-silver')
output_file = os.path.join(silver_folder, 'VIX.csv')

# Save the modified DataFrame to a CSV file in the "silver" folder
df.to_csv(output_file, index=False)

# Display message
print('Volatility Index (VIX) data processed and saved to silver layer')