import subprocess

# List of scripts to run in order
scripts_to_run = [
    '01_combine_spotify_data.py',
    # Add other script names here in the order they should be run
    # '02_another_script.py',
    # '03_yet_another_script.py',
]

# Loop through and run each script
for script in scripts_to_run:
    try:
        print(f"Running {script}...")
        subprocess.run(['python', script], check=True)
        print(f"{script} completed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while running {script}: {e}")
        break  # Stop running further scripts if one fails
    except FileNotFoundError:
        print(f"Script {script} not found.")
        break

print("Data ingestion process completed.")