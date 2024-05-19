from combine_spotify_csvs_01 import combine_spotify_csv
from process_spotify_data_02 import process_spotify_csv


def run_all_ingestion():
    # Run the combine_spotify_csv function
    output_file_path = combine_spotify_csv()
    output_file_path = process_spotify_csv(output_file_path)


    # Add code to run other ingestion processes if needed
    # For example:
    # process_other_data(output_file_path)

if __name__ == "__main__":
    run_all_ingestion()