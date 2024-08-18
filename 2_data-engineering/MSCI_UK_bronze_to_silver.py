import pandas as pd
import os
import sys

def main(days, effect_days, bronze_data_folder, silver_data_folder):   

    days = int(days)
    effect_days = int(effect_days)

    # Define the path to the CSV file
    csv_file = os.path.join(bronze_data_folder, 'downloaded_MSCI_UK.csv')

    # prepare dates that exist in ftse 
    FTSE_file = os.path.join(bronze_data_folder, 'downloaded_FTSE100.csv')
    FTSE = pd.read_csv(FTSE_file)
    FTSE['Date'] = pd.to_datetime(FTSE['date'], dayfirst=True)
    FTSE.set_index('Date', inplace=True)
    # Load the CSV file into a pandas DataFrame
    df = pd.read_csv(csv_file)

    # Ensure the date column is in datetime format
    df['Date'] = pd.to_datetime(df['Date'], dayfirst=True)

    # Filter df to only include rows where the 'Date' is in ftse's index
    df = df[df['Date'].isin(FTSE.index)]

    # Sort the DataFrame by date if it's not already sorted
    df = df.sort_values(by='Date')

    # Set the date column as the index
    df.set_index('Date', inplace=True)

    # Shift the 'Price' column to get the lagged data
    df['Previous Price'] = df['Price'].shift(days)

    # Calculate the percentage change from the previous week's closing
    df['% MSCIUK Change'] = ((df['Price'] - df['Previous Price']) / df['Previous Price']) * 100

    # Round up % MSCIUK Change to 2 decimal places
    df['% MSCIUK Change'] = df['% MSCIUK Change'].round(2)

    df['Previous % MSCIUK Change'] = df['% MSCIUK Change'].shift(days)

    df['Next % MSCIUK Change'] = df['% MSCIUK Change'].shift(-effect_days)

    # Keep only relevant columns
    result_df = df[['% MSCIUK Change', 'Previous % MSCIUK Change', 'Next % MSCIUK Change']]

    # Drop rows with NaN values 
    result_df = result_df.copy()
    result_df.dropna(inplace=True)

    # Reset the index to have a clean DataFrame
    result_df.reset_index(inplace=True)

    # Define the path to save the new CSV file in the "silver" folder
    output_file = os.path.join(silver_data_folder, 'MSCIUK.csv')

    # Save the new DataFrame to a CSV file in the "silver" folder
    result_df.to_csv(output_file, index=False)

    # Display message
    print('___MSCIUK data processed and saved to silver layer')
    
    
if __name__ == "__main__":
    if len(sys.argv) > 3:
        days = sys.argv[1]
        effect_days = sys.argv[2]
        bronze_data_folder = sys.argv[3]
        silver_data_folder = sys.argv[4]

        main(days, effect_days, bronze_data_folder, silver_data_folder)
    else:
        print("No parameters provided.")