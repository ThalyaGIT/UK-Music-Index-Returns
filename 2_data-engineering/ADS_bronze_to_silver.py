import pandas as pd
import os
import sys

def main(days, bronze_data_folder, silver_data_folder):   

    days = int(days)

    # Prepare Dates that only exist in FTSE 100
    FTSE_file = os.path.join(bronze_data_folder, 'downloaded_FTSE100.csv')
    FTSE = pd.read_csv(FTSE_file)
    FTSE['Date'] = pd.to_datetime(FTSE['date'], dayfirst=True)
    FTSE.set_index('Date', inplace=True)

    # Load the ADS data into a pandas DataFrame       
    ADS_file = os.path.join(bronze_data_folder, 'downloaded_ADS.csv')
    df = pd.read_csv(ADS_file)

    # Ensure the Date column is in datetime format
    df['Date'] = pd.to_datetime(df['Date'], format="%Y-%m-%d")
    df = df.sort_values(by='Date')

    # Filter df to only include rows where the 'Date' is in FTSE 100 index
    df = df[df['Date'].isin(FTSE.index)]

    df.set_index('Date', inplace=True)

    # Shift the 'ADS Index' column to get the lagged data
    df['Previous ADS Index'] = df['ADS Index'].shift(days)

    # Calculate the ADS_Change
    df['ADS_Change'] = df['ADS Index'] - df['Previous ADS Index']

    # Save DF into Silver Data CSV
    result_df = df[['ADS_Change']]
    result_df = result_df.copy()
    result_df.dropna(inplace=True)
    result_df.reset_index(inplace=True)
    silver_data = os.path.join(silver_data_folder, 'ADS.csv')
    result_df.to_csv(silver_data, index=False)

    print('____ADS data processed and saved to silver layer')

if __name__ == "__main__":
    if len(sys.argv) > 2:
        param1 = sys.argv[1]
        bronze_data_folder = sys.argv[3]
        silver_data_folder = sys.argv[4]
        main(param1, bronze_data_folder , silver_data_folder)
    else:
        print("No parameters provided.")
