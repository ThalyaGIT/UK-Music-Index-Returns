import pandas as pd
import os
import sys

def main(days, bronze_data_folder, silver_data_folder):   

    days = int(days)

    # Paths
    gold_path = os.path.join(os.path.dirname(__file__), '..', '0-data-gold')                    

    # List of CSV files in the 'silver' folder
    csv_files = [  'ADS.csv'
                , 'EPU.csv'               
                , 'COVID.csv'
                , 'Cloud.csv'                
                , 'FTSE100.csv'
                , 'FTSE250.csv'
                ,  'FTSEAIM.csv'
                , 'FTSEAllShare.csv'
                , 'FTSESmallCap.csv'
                , 'MSCI.csv'
                , 'MSCIUK.csv'
                , 'GILT2.csv'
                , 'VIX.csv'
                , 'TED.csv'
                , 'Spotify.csv'
                ]

    # Initialize an empty list to store DataFrames
    dfs = []

    # Read each CSV file and append the DataFrame to the list
    for file in csv_files:
        file_path = os.path.join(silver_data_folder, file)
        df = pd.read_csv(file_path, parse_dates=['Date'])  # Ensure the 'Date' column is parsed as datetime
        dfs.append(df)

    # Merge all DataFrames on the 'Date' column
    merged_df = dfs[0]
    for df in dfs[1:]:
        merged_df = merged_df.merge(df, on='Date', how='outer', suffixes=('', '_dup')).drop([col for col in merged_df.columns if '_dup' in col], axis=1)

    # Sort the DataFrame by Date
    merged_df = merged_df.sort_values(by='Date')

    # # Identify rows with null values
    # rows_with_nulls = merged_df[merged_df.isnull().any(axis=1)]

    # Remove rows with null values
    cleaned_df = merged_df.dropna()

    # Save the cleaned DataFrame to a CSV file in the 'gold' folder
    output_file_path = os.path.join(gold_path, f'data_{days}_days.csv')
    cleaned_df.to_csv(output_file_path, index=False)

    print(f"____DONE: Merged data in Gold layer as data_{days}_days.csv")
    
if __name__ == "__main__":
    if len(sys.argv) > 2:
        param1 = sys.argv[1]
        bronze_data_folder = sys.argv[2]
        silver_data_folder = sys.argv[3]
        main(param1, bronze_data_folder , silver_data_folder)
    else:
        print("No parameters provided.")

