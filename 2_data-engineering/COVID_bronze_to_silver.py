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

    # Load the CSV file into a pandas DataFrame
    COVID_file = os.path.join(bronze_data_folder, 'downloaded_COVID.csv')
    df = pd.read_csv(COVID_file, low_memory=False)

    # Convert 'Date' column to datetime format
    df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d')

    # Filter df to only include rows where the 'Date' is in ftse's index
    df = df[df['Date'].isin(FTSE.index)]

    # Filter on data for GB
    df = df[df['CountryCode'] == 'GBR']

    # Filter on data for all regions
    df = df[df['RegionCode'].isna()]

    # Keep Columns you need
    df = df[['Date'
            , 'C1M_School closing'
            , 'C2M_Workplace closing'
            , 'C3M_Cancel public events'
            , 'C4M_Restrictions on gatherings'
            , 'C5M_Close public transport'
            , 'C6M_Stay at home requirements'
            ]]

    df['Covid_Stringency'] = df.iloc[:, 1:].sum(axis=1)

    df = df.sort_values(by='Date')

    # Set the date column as the index
    df.set_index('Date', inplace=True)

    df['Previous_Covid_Stringency'] = df['Covid_Stringency'].shift(days)

    df['Stringency_Change'] = df['Covid_Stringency'] - df['Previous_Covid_Stringency']

    df = df[['Stringency_Change']]

    # Reindex the df to have all dates present in the FTSE100 DataFrame
    df = df.reindex(FTSE.index)

    # Fill missing values with 0
    df = df.fillna(0)

    # Drop rows with NaN values 
    df.dropna(inplace=True)

    # Reset the index to have a clean DataFrame
    df.reset_index(inplace=True)

    # Define the path to save the new CSV file in the "silver" folder
    output_file = os.path.join(silver_data_folder, 'COVID.csv')

    # Save the new DataFrame to a CSV file in the "silver" folder
    df.to_csv(output_file, index=False)

    # Display message
    print('___Covid Stringency data processed and saved to silver layer')
    
if __name__ == "__main__":
    if len(sys.argv) > 2:
        param1 = sys.argv[1]
        bronze_data_folder = sys.argv[3]
        silver_data_folder = sys.argv[4]
        main(param1, bronze_data_folder , silver_data_folder)
    else:
        print("No parameters provided.")

