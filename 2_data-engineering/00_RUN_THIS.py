import subprocess
import os

# Define the folder paths
script_folder = os.path.join(os.path.dirname(__file__))
bronze_data_folder = os.path.join(os.path.dirname(__file__), '..', '0_data-bronze')
silver_data_folder = os.path.join(os.path.dirname(__file__), '..', '0-data-silver')

# List of script names
scripts = [
     'ADS_bronze_to_silver.py',
    'Cloud_bronze_to_silver.py',
    'COVID_bronze_to_silver.py',
    'EPU_bronze_to_silver.py',
    'FTSE_100_bronze_to_silver.py',
    'FTSE_250_bronze_to_silver.py',
    'FTSE_AIM_bronze_to_silver.py',
    'FTSE_AllShare_bronze_to_silver.py',
    'FTSE_SmallCap_bronze_to_silver.py',
    'GILT2_bronze_to_silver.py',
    'HOLD_bronze_to_silver.py',
    'MSCI_bronze_to_silver.py',
    'MSCI_UK_bronze_to_silver.py',
    'TED_bronze_to_silver.py',
    'VIX_bronze_to_silver.py',
    'Spotify_bronze_to_silver.py',
    'ZZ_Join_datasets_silver_to_gold.py',
    'ZZ_Join_datasets_silver_to_gold_with_TED.py'
]

# Function to run each script
def run_script(script_name, days, effect_days, bronze_data_folder, silver_data_folder):
    script_path = os.path.join(script_folder, script_name)
    result = subprocess.run(['python', script_path, str(days), str(effect_days), bronze_data_folder, silver_data_folder])
    if result.returncode != 0:
        print(f'{script_name} failed with return code {result.returncode}')
    else:
        print(f'{script_name} completed successfully with days={days} and effect_days={effect_days}')

# Function to loop over all scripts with given days and effect_days
def run_all_scripts(scripts, days_list, effect_days, bronze_data_folder, silver_data_folder):
    for days in days_list:
        for script in scripts:
            run_script(script, days, effect_days, bronze_data_folder, silver_data_folder)

# List of days to loop over
days_list = [1,2]
effect_days = 5  # Set effect_days value

# Run all scripts with the given parameters
run_all_scripts(scripts, days_list, effect_days, bronze_data_folder, silver_data_folder)