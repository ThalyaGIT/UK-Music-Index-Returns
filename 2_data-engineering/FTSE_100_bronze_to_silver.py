import pandas as pd
import os
import sys


def main(days, bronze_data_folder, silver_data_folder):   

    days = int(days)

    # Define the path to the CSV file
    FTSE100_file = os.path.join(bronze_data_folder, 'downloaded_FTSE100.csv')

    # Load the CSV file into a pandas DataFrame
    df = pd.read_csv(FTSE100_file)

    # Ensure the date column is in datetime format
    df['Date'] = pd.to_datetime(df['date'],dayfirst=True)

    # Sort the DataFrame by date if it's not already sorted
    df = df.sort_values(by='Date')

    # Set the date column as the index
    df.set_index('Date', inplace=True)

    # Shift the 'Price' column to get the lagged data
    df['Previous Price'] = df['Price'].shift(days)

    # Calculate the percentage change from the previous week's closing
    df['% FTSE100 Change'] = ((df['Price'] - df['Previous Price']) / df['Previous Price']) * 100

    # Round up % FTSE100 Change to 2 decimal places
    df['% FTSE100 Change'] = df['% FTSE100 Change'].round(2)

    df['Previous % FTSE100 Change'] = df['% FTSE100 Change'].shift(days)

    df['Next % FTSE100 Change'] = df['% FTSE100 Change'].shift(-days)

    # Keep only relevant columns
    result_df = df[['% FTSE100 Change', 'Previous % FTSE100 Change', 'Next % FTSE100 Change']]

    # Drop rows with NaN values
    result_df = result_df.copy()
    result_df.dropna(inplace=True)

    # Reset the index to have a clean DataFrame
    result_df.reset_index(inplace=True)

    # Define the path to save the new CSV file in the "silver" folder
    output_file = os.path.join(silver_data_folder, 'FTSE100.csv')

    # Save the new DataFrame to a CSV file in the "silver" folder
    result_df.to_csv(output_file, index=False)

    # Display message
    print('___FTSE 100 data processed and saved to silver layer')
    
if __name__ == "__main__":
    if len(sys.argv) > 2:
        param1 = sys.argv[1]
        bronze_data_folder = sys.argv[2]
        silver_data_folder = sys.argv[3]
        main(param1, bronze_data_folder , silver_data_folder)
    else:
        print("No parameters provided.")
