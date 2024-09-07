import pandas as pd
import os
import sys

def main(days, bronze_data_folder, silver_data_folder):   

    days = int(days)

    # prepare dates that exist in ftse 
    FTSE_file = os.path.join(bronze_data_folder, 'downloaded_FTSE100.csv')
    FTSE = pd.read_csv(FTSE_file)
    FTSE['Date'] = pd.to_datetime(FTSE['date'], dayfirst=True)
    FTSE = FTSE[FTSE['Date'].dt.weekday == 4]
    FTSE.set_index('Date', inplace=True)

    # Load the CSV file into a pandas DataFrame
    EPU_file = os.path.join(bronze_data_folder, 'downloaded_EPU.csv')
    df = pd.read_csv(EPU_file)

    # Ensure the date column is in datetime format
    df['Date'] = pd.to_datetime(df['Date'])

    # Sort the DataFrame by date if it's not already sorted
    df = df.sort_values(by='Date')

    # Filter df to only include rows where the 'Date' is in ftse's index
    df = df[df['Date'].isin(FTSE.index)]

    # Set the date column as the index
    df.set_index('Date', inplace=True)

    # Shift the 'EPU Index' column to get the lagged data
    df['Previous EPU Index'] = df['EPU Index'].shift(days)

    # Calculate the EPU_Change from the previous week's closing
    df['EPU_Change'] = df['EPU Index'] - df['Previous EPU Index']

    # Keep only relevant columns
    result_df = df[['EPU_Change']]
    
    # Drop rows with NaN values 
    result_df = result_df.copy()
    result_df.dropna(inplace=True)

    # Reset the index to have a clean DataFrame
    result_df.reset_index(inplace=True)

    # Save the new DataFrame to a CSV file in the "silver" folder
    silver_file = os.path.join(silver_data_folder, 'EPU.csv')
    result_df.to_csv(silver_file, index=False)

    # Display message
    print('___Economic Policy Uncertainty (EPU) data processed and saved to silver layer')
    
if __name__ == "__main__":
    if len(sys.argv) > 2:
        param1 = sys.argv[1]
        bronze_data_folder = sys.argv[3]
        silver_data_folder = sys.argv[4]
        main(param1, bronze_data_folder , silver_data_folder)
    else:
        print("No parameters provided.")

