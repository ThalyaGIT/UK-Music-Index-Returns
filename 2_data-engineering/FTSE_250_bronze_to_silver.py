import pandas as pd
import os
import sys


def main(days, effect_days, bronze_data_folder, silver_data_folder):   

    days = int(days)
    effect_days = int(effect_days)
        
    # Define the path to the CSV file
    FTS250_file = os.path.join(bronze_data_folder, 'downloaded_FTSE250.csv')

    # Load the CSV file into a pandas DataFrame
    df = pd.read_csv(FTS250_file)

    # Ensure the date column is in datetime format
    df['Date'] = pd.to_datetime(df['Date'], dayfirst=True)
    df = df[df['Date'].dt.weekday == 4]

    # Sort the DataFrame by date if it's not already sorted
    df = df.sort_values(by='Date')

    # Set the date column as the index
    df.set_index('Date', inplace=True)

    # Shift the 'Price' column to get the lagged data
    df['Previous Price'] = df['Price'].shift(days)

    # Calculate the percentage change from the previous week's closing
    df['% FTSE250 Change'] = ((df['Price'] - df['Previous Price']) / df['Previous Price']) * 100

    # Round up % FTSE250 Change to 2 decimal places
    df['% FTSE250 Change'] = df['% FTSE250 Change'].round(2)

    df['Previous % FTSE250 Change'] = df['% FTSE250 Change'].shift(days)

    df['Next % FTSE250 Change'] = df['% FTSE250 Change'].shift(-days)

    # Keep only relevant columns
    result_df = df[['% FTSE250 Change', 'Previous % FTSE250 Change', 'Next % FTSE250 Change']]

    # Drop rows with NaN values 
    result_df = result_df.copy()
    result_df.dropna(inplace=True)

    # Reset the index to have a clean DataFrame
    result_df.reset_index(inplace=True)

    # Define the path to save the new CSV file in the "silver" folder
    silver_data_folder = os.path.join(os.path.dirname(__file__), '..', '0-data-silver')
    output_file = os.path.join(silver_data_folder, 'FTSE250.csv')

    # Save the new DataFrame to a CSV file in the "silver" folder
    result_df.to_csv(output_file, index=False)

    # Display message
    print('___FTSE 250 data processed and saved to silver layer')
    
    
if __name__ == "__main__":
    if len(sys.argv) > 3:
        days = sys.argv[1]
        effect_days = sys.argv[2]
        bronze_data_folder = sys.argv[3]
        silver_data_folder = sys.argv[4]

        main(days, effect_days, bronze_data_folder, silver_data_folder)
    else:
        print("No parameters provided.")