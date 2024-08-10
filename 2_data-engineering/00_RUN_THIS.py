import subprocess
import os

folder = os.path.join(os.path.dirname(__file__))

# List of script names
scripts = ['ADS_bronze_to_silver.py'
           , 'Cloud_bronze_to_silver.py'
           , 'COVID_bronze_to_silver.py'
           , 'EPU_bronze_to_silver.py'
           , 'FTSE_bronze_to_silver.py'
           , 'FTSE_250_bronze_to_silver.py'
           , 'FTSE_All_Share_bronze_to_silver.py'
           , 'FTSE_Small_Cap_bronze_to_silver.py'
           , 'MSCI_UK_bronze_to_silver.py'
           , 'MSCI_bronze_to_silver.py'
           , 'Spotify_bronze_to_silver.py'
           , 'VIX_bronze_to_silver.py'
           , 'Join_datasets_silver_to_gold.py'
           ]

# Run each script
for script in scripts:
    script_path = os.path.join(folder, script)
    result = subprocess.run(['python', script_path])
    if result.returncode != 0:
        print(f'{script} failed with return code {result.returncode}')