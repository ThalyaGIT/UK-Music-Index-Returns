import pandas as pd
import os
import sys

def process_file(days, effect_days, bronze_data_folder, silver_data_folder, file_name):
    # Extract the company name from the file name (assumed to be the part after "Hold_")
    company_name = file_name.split('_')[-1].split('.')[0]

    days = int(days)
    effect_days = int(effect_days)

    # Define the path to the CSV file
    file_path = os.path.join(bronze_data_folder, file_name)

    # Load the CSV file into a pandas DataFrame
    df = pd.read_csv(file_path)

    # Ensure the date column is in datetime format
    df['Date'] = pd.to_datetime(df['Date'], dayfirst=True) 

    # Sort the DataFrame by date if it's not already sorted
    df = df.sort_values(by='Date')

    # Set the date column as the index
    df.set_index('Date', inplace=True)

    # Shift the 'Price' column to get the lagged data
    df['Previous Price'] = df['Price'].shift(days)

    # Calculate the percentage change from the previous week's closing
    df[f'% {company_name} Change'] = ((df['Price'] - df['Previous Price']) / df['Previous Price']) * 100

    # Round up percentage change to 2 decimal places
    df[f'% {company_name} Change'] = df[f'% {company_name} Change'].round(2)

    df[f'Previous % {company_name} Change'] = df[f'% {company_name} Change'].shift(days)

    df[f'Next % {company_name} Change'] = df[f'% {company_name} Change'].shift(-effect_days)

    # Keep only relevant columns
    result_df = df[[f'% {company_name} Change', f'Previous % {company_name} Change', f'Next % {company_name} Change']]

    # Drop rows with NaN values 
    result_df = result_df.copy()
    result_df.dropna(inplace=True)

    # Reset the index to have a clean DataFrame
    result_df.reset_index(inplace=True)

    # Define the path to save the new CSV file in the "silver" folder
    output_file = os.path.join(silver_data_folder, f'{company_name}.csv')

    # Save the new DataFrame to a CSV file in the "silver" folder
    result_df.to_csv(output_file, index=False)

    # Display message
    print(f'___{company_name} data processed and saved to silver layer')

def main(days, effect_days, bronze_data_folder, silver_data_folder):
    # List of files to process
    file_names = [
        'downloaded_hold_Barc.csv',
        'downloaded_hold_Glen.csv',
        'downloaded_hold_Voda.csv',
        'downloaded_hold_LLoyds.csv',
        'downloaded_hold_BP.csv'
    ]

    # Process each file in the list
    for file_name in file_names:
        process_file(days, effect_days, bronze_data_folder, silver_data_folder, file_name)

if __name__ == "__main__":
    if len(sys.argv) > 3:
        days = sys.argv[1]
        effect_days = sys.argv[2]
        bronze_data_folder = sys.argv[3]
        silver_data_folder = sys.argv[4]

        main(days, effect_days, bronze_data_folder, silver_data_folder)
    else:
        print("No parameters provided.")