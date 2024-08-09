import pandas as pd
import statsmodels.api as sm
import os

# Read the CSV file
# Paths
gold_path = os.path.join(os.path.dirname(__file__), '..', '0-data-gold')   
csv_file = os.path.join(gold_path, 'data.csv')

# Load the CSV file into a pandas DataFrame
df = pd.read_csv(csv_file)

# Ensure the 'End of Week Date' column is in datetime format if necessary
df['Date'] = pd.to_datetime(df['Date'])

# Define the start and end dates for the filter
start_date = '2017-01-01'
end_date = '2023-12-31'

# Filter the DataFrame for dates within the specified range
df = df[(df['Date'] >= start_date) & (df['Date'] <= end_date)]

# # Define the dependent variable
y = df['% FTSE100 Change']

# # Define the independent variables, including 'Change in SWAV' and the other control variables
X = df[[ 'Change in SWAV'                            # Independent Variable: Contemporaneous
         , 'ADS_Change'                              # Control 1: Business Conditions
         , 'EPU_Change'                              # Control 2 : Economic Policy Uncertainty 
         , 'Previous Week % FTSE100 Change'          # Control 3: Previous FTSE10
         , '% MSCI Change'                           # Control 4: World Index
         , 'Vix Close'                               # Control 5: Volaitlity
         , '7D_Rolling_Avg_Change_in_DCC'            # Control 6: Cloud Cover
         , 'Stringency_Change'                       # Control 7: Covid Rules Stringency
        ]]

# Add a constant to the model (intercept)
X = sm.add_constant(X)

# Build and fit the regression model
model = sm.OLS(y, X).fit()

# Print the summary of the regression model
print(model.summary())