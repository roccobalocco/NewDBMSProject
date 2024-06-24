from add_frauds import add_frauds
from generate_dataset import generate_dataset
import os
import datetime

# 5000 10000 183 are, more or less, 100mb of data

args_num:dict = {0: [2500, 5000, 92], 1: [5000, 10000, 183], 2: [10000, 20000, 365]}

for key, value in args_num.items():
    # Generare dataset for the three tables
    (customer_profiles_table, terminal_profiles_table, transactions_df)=\
        generate_dataset(n_customers = value[0], 
                        n_terminals = value[1], 
                        nb_days= value[2], 
                        start_date="2018-04-01", 
                        r=5)

    # Add frauds to transactions
    transactions_df = add_frauds(customer_profiles_table, terminal_profiles_table, transactions_df)

    # Saving of dataset
    DIR_OUTPUT = "./simulated-data-raw"
    if key == 0:
        DIR_OUTPUT += '-50mb/'
    elif key == 1:
        DIR_OUTPUT += '-100mb/'
    else: 
        DIR_OUTPUT += '-200mb/'

    if not os.path.exists(DIR_OUTPUT):
        os.makedirs(DIR_OUTPUT)

    start_date = datetime.datetime.strptime("2018-04-01", "%Y-%m-%d")

    for day in range(transactions_df.TX_TIME_DAYS.max()+1):
        
        transactions_day = transactions_df[transactions_df.TX_TIME_DAYS==day].sort_values('TX_TIME_SECONDS')
        
        date = start_date + datetime.timedelta(days=day)
        filename_output = date.strftime("%Y-%m-%d")+'.pkl'
        
        # Protocol=4 required for Google Colab
        transactions_day.to_pickle(DIR_OUTPUT+filename_output, protocol=4)