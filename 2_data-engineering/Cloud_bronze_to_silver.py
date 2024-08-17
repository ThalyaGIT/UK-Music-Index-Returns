import pandas as pd
import os
import sys

def main(days, bronze_data_folder, silver_data_folder):   

    days = int(days)

    # prepare dates that exist in ftse 
    FTSE_file = os.path.join(bronze_data_folder, 'downloaded_FTSE100.csv')
    FTSE = pd.read_csv(FTSE_file)
    FTSE['Date'] = pd.to_datetime(FTSE['date'], dayfirst=True)
    FTSE.set_index('Date', inplace=True)

    # Load your cloud cover data
    Cloud_file = os.path.join(bronze_data_folder, 'downloaded_Cloud.csv')
    data  = pd.read_csv(Cloud_file)

    # Convert 'Date' to datetime format and 'Time' to string format
    data['Date'] = pd.to_datetime(data['Date'], format='%d/%m/%Y')
    data['Time'] = data['Time'].astype(str)

    # Filter the data to include only times between 08:00 and 17:00
    filtered_data = data[(data['Time'] >= '08:00:00') & (data['Time'] <= '17:00:00')].copy()

    # First , get the DCC!
    # Calculate the daily average cloud cover
    data = filtered_data.groupby('Date', as_index=False)['Cloud_Cover'].mean()

    # Filter df to only include rows where the 'Date' is in ftse's index
    data = data[data['Date'].isin(FTSE.index)]

    # Calculate the Rolling-days-Average-Cloud-Cover
    data['Rolling_Avg_Cloud_Cover'] = data['Cloud_Cover'].rolling(window=days, min_periods=1).mean()

    # Deseasonalize the cloud cover by substracting Rolling-days-Average-Cloud-Cover
    data['DCC'] = data['Cloud_Cover'] - data['Rolling_Avg_Cloud_Cover'] 

    # Next, get the average daily change in deseasonalized cloud cover within a week
    # Calculate daily changes in DCC  (Today minus yesteday's DCC)
    data['Previous_Day_DCC'] = data['DCC'].shift(1)
    data['Change_in_DCC'] = data['DCC']- data['Previous_Day_DCC']

    # Calculate the average change in DCC for the past whatever days
    data['Rolling_Avg_Change_in_DCC'] = data['Change_in_DCC'].rolling(window=days, min_periods=1).mean()

    final_data = data[['Date','Rolling_Avg_Change_in_DCC']].drop_duplicates()

    # Define the path to save the new CSV file in the "silver" folder
    output_file = os.path.join(silver_data_folder, 'Cloud.csv')

    # Save the new DataFrame to a CSV file in the "silver" folder
    final_data.to_csv(output_file, index=False)

    # Display message
    print('___Deseasonalized Cloud Cover (DCC) data processed and saved to the silver layer.')

if __name__ == "__main__":
    if len(sys.argv) > 2:
        param1 = sys.argv[1]
        bronze_data_folder = sys.argv[2]
        silver_data_folder = sys.argv[3]
        main(param1, bronze_data_folder , silver_data_folder)
    else:
        print("No parameters provided.")


