import pandas as pd
import os
import sys

def main( bronze_data_folder, silver_data_folder):   

    # Define the path to the CSV file
    csv_file = os.path.join(bronze_data_folder, 'downloaded_VIX.csv')

    # prepare dates that exist in ftse 
    FTSE_file = os.path.join(bronze_data_folder, 'downloaded_FTSE100.csv')
    FTSE = pd.read_csv(FTSE_file)
    FTSE['Date'] = pd.to_datetime(FTSE['date'], dayfirst=True)
    FTSE.set_index('Date', inplace=True)

    # Load the CSV file into a pandas DataFrame
    df = pd.read_csv(csv_file)

    # Ensure the date column is in datetime format
    df['Date'] = pd.to_datetime(df['Date'])
    df = df[df['Date'].dt.weekday == 4]

    # Filter df to only include rows where the 'Date' is in ftse's index
    df = df[df['Date'].isin(FTSE.index)]

    # Drop columns 'Open', 'High', and 'Low' if they exist
    columns_to_drop = ['Open', 'High', 'Low']
    df.drop(columns=columns_to_drop, inplace=True, errors='ignore')

    df['Vix Close'] = df['Close'] 

    # Define the path to save the new CSV file in the "silver" folder
    output_file = os.path.join(silver_data_folder, 'VIX.csv')

    # Save the modified DataFrame to a CSV file in the "silver" folder
    df.to_csv(output_file, index=False)

    # Display message
    print('___VIX data processed and saved to silver layer')
     
if __name__ == "__main__":
    if len(sys.argv) > 2:
        bronze_data_folder = sys.argv[3]
        silver_data_folder = sys.argv[4]
        main( bronze_data_folder , silver_data_folder)
    else:
        print("No parameters provided.")