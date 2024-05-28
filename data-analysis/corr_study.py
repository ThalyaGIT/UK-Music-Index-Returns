import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

file_path = os.path.join('data-ingestion', 'data','Spotify', '07_join_ftse100_spotify.csv')

# Convert the relative path to an absolute path
absolute_path = os.path.abspath(file_path)

# Print the absolute path for debugging
print(f"Absolute path to the file: {absolute_path}")

# Read the merged CSV file
merged_df = pd.read_csv(file_path)

# Calculate the correlation coefficient
correlation = merged_df['average_valence'].corr(merged_df['Change %'])
print(f'Correlation coefficient between average valence and change%: {correlation}')

# Plot the correlation graph
plt.figure(figsize=(10, 6))
sns.regplot(x='average_valence', y='Change %', data=merged_df, scatter_kws={'s':10})
plt.title('Correlation between Average Valence and Change%')
plt.xlabel('Average Valence')
plt.ylabel('Change%')
plt.grid(True)
plt.show()