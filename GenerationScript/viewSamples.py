
# Generare dataset for the three tables
from add_frauds import add_frauds
from generate_dataset import generate_dataset


(customer_profiles_table, terminal_profiles_table, transactions_df)=\
    generate_dataset(n_customers = 10, 
                    n_terminals = 4, 
                    nb_days= 7, 
                    start_date="2018-04-01", 
                    r=5)

# Add frauds to transactions
transactions_df = add_frauds(customer_profiles_table, terminal_profiles_table, transactions_df)

print(customer_profiles_table.head())
print(terminal_profiles_table.head())

transactions_df = transactions_df.sort_values("TX_FRAUD", ascending=False)  # Fix: Use sort_values() instead of sort_value()
print(transactions_df.head())
