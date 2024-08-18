import pandas as pd
import os
import sys

def main( bronze_data_folder, silver_data_folder):   

    # Define the path to the CSV file
    csv_file = os.path.join(bronze_data_folder, 'downloaded_TED.csv')

    # Load the CSV file into a pandas DataFrame
    df = pd.read_csv(csv_file)

    # Ensure the date column is in datetime format
    df['Date'] = pd.to_datetime(df['Date'], format="%d/%m/%Y")

    # Rename 'Value' column to 'TED'
    df.rename(columns={'Value': 'TED'}, inplace=True)

    # Sort the DataFrame by date if it's not already sorted
    df = df.sort_values(by='Date')

    # Set the date column as the index
    df.set_index('Date', inplace=True)

    # Drop rows with NaN values 
    df.dropna(inplace=True)

    # Reset the index to have a clean DataFrame
    df.reset_index(inplace=True)

    # Define the path to save the new CSV file in the "silver" folder
    output_file = os.path.join(silver_data_folder, 'TED.csv')

    # Save the new DataFrame to a CSV file in the "silver" folder
    df.to_csv(output_file, index=False)

    # Display message
    print('___TED data processed and saved to silver layer')
       
if __name__ == "__main__":
    if len(sys.argv) > 2:
        bronze_data_folder = sys.argv[3]
        silver_data_folder = sys.argv[4]
        main(bronze_data_folder , silver_data_folder)
    else:
        print("No parameters provided.")


