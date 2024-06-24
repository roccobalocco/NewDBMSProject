from pandas import DataFrame
from add_frauds import add_frauds
from generate_dataset import generate_dataset
import os
import datetime

def create_dir(dirname: str):
    if not os.path.exists(dirname):
        os.makedirs(dirname)
    
# 5000 10000 183 are, more or less, 100mb of data

args_num:dict = {0: [5000, 8500, 92], 1: [6000, 11000, 183], 2: [8000, 15000, 365]}

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

    create_dir(DIR_OUTPUT)

    start_date = datetime.datetime.strptime("2018-04-01", "%Y-%m-%d")

    # saving customers
    customer_profiles_table.to_csv(DIR_OUTPUT + '/customers.csv', sep=',', encoding='utf-8')
    
    # saving terminals   
    terminal_profiles_table.to_csv(DIR_OUTPUT + '/terminals.csv', sep=',', encoding='utf-8')

    # saving transactions:
    transactions_df.to_csv(DIR_OUTPUT + '/transactions.csv', sep=',', encoding='utf-8')