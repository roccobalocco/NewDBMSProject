
# Generare dataset for the three tables
from add_frauds import add_frauds
from generate_dataset import generate_dataset


(customer_profiles_table, terminal_profiles_table, transactions_df)=\
    generate_dataset(n_customers = 250, 
                    n_terminals = 500, 
                    nb_days= 90, 
                    start_date="2018-04-01", 
                    r=5)

# Add frauds to transactions
transactions_df = add_frauds(customer_profiles_table, terminal_profiles_table, transactions_df)

print(customer_profiles_table.head())
print(terminal_profiles_table.head())
print(transactions_df.head())
