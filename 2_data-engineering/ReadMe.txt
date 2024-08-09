Once the raw data has been downloaded or ingested by our Ingestion engine, 
they are saved in the bronze layer.

The data engineering engine cleans and transforms each dataset and saves it in our silver layer.

In each file, the date column is unique and covers 2017-01-01 till 2023-12-31

Dependent Variable:
FTSE columns are "Price" , "Previous Week % FTSE100 Change" (7 days), "% FTSE100 Change" 

Independent Variable:
Spotify columns are "SWAV", "lagged_SWAV" (7 days), "Change in SWAV"
ADS, tracks business conditions and it's columns are "ADS Index", "Previous ADS Index" , "ADS_Change"
EPU, Economic Policy Uncertainty columns are "Date" , "EPU Index" ,"Previous EPU Index" , "EPU_Change" 




Once that is done, we join all datasets by the date and save the csv in the gold layer, we call this file
data.csv This is the  csv file that we will use for regression.






