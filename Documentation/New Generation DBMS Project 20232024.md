# New Generation DBMS Project 2023/2024:

For the realization of this project I have choose to use Neo4J, the choice of a Graph Database is due to the value that the relationship assumed in the considered dataset and to some of the operations that are needed.

## Models:

The simulator generate three different tables:

- **Customer**, that doesn't hold any classical information (like the name, surname, etc) about the person that it represents, but instead has a more pragmatical way of describe it with a *unique identifier*, the *geographical location*, the *spending frequency* and the *spending amounts*
- **Terminal**, that holds only the information about his *geographical location*
- **Transaction**, that connects a **Customer** and **Terminal**, holding the information about the buying action. So transaction contains the *customer identifier*, the *terminal identifier*, the *amount of the transaction*, the *date in which the transaction occurred* and an optional field that marks a **Transaction** as *fraudulent*

### Conceptual Model:

The conceptual model is pretty simple, it lists the three entities as we see them in the dataset, connecting them via relationship of (1:n) kind.

So there will be a **Customer** that can make many **Transactions** and each of them referrer to one **Terminal**.

![image-20240704144009187](/home/pietro/.var/app/io.typora.Typora/config/Typora/typora-user-images/image-20240704144009187.png)

### Logical Model:

The initial columns are the same proposed by the simulator.

![image-20240704152502540](./assets/image-20240704152502540.png)

### Physical Model:

In the physical model I have added the types, the primary key and the foreign keys at the logical model.

![image-20240704153211345](./assets/image-20240704153211345.png)

### UML Class Diagram:

The **Transaction** entity has been transformed into an *association class* because it doesn't exists by itself, but it only exists if binded with both **Customer** and **Terminal**. 

![image-20240704162745584](./assets/image-20240704162745584.png)

By considering the operations that are going to extend our entities the class diagram has been transformed into this one, who add three new properties to **Transaction**, that are:

- `KIND_OF_PRODUCT`
- `FEELING_OF_SECURITY`
- `PERIOD_OF_DAY`

![image-20240704162929756](./assets/image-20240704162929756.png)

## Neo4J:

Starting from the last UML Class Diagram, the nodes and the relationships in Neo4J reflect perfectly his structure. In the database we will have the following Nodes:

- `Customer`, with all the properties listed in the diagram
- `Terminal`, with all the properties listed in the diagram

And the following Relationships:

- `Transaction`, that starts from a `Customer` and a ends to a `Terminal`, with the same properties listed in the diagram
-  `Buying_friends`, that connects two `Customer` without any properties

Moreover, the operations request to identify a special relationship between `Customer`, the `Co-customer` relationship; this will be available on call, so it won't be showed in the database, but only on request as result of the request.

### Consideration and constraints:

#### Consideration:

I have not changed nothing to the initial dataset in term of properties, neither in terms of data structures, but considering the operations that were given and the NoSQL database chosen there weren't particular changing that were really "mandatory" or "game changing".

About the operation, I have to clarify my interpretation for each of them:

1. Operation **a**:

   > For each customer checks that the spending frequency and the spending amounts of the last month is under the usual spending frequency and the spending amounts for the same period.

   Get customer under average for spending amounts and frequency of spending in the last month, by comparing them to the average of this period (considering period as the same day&month over all the years registered in the database).
   The average is computed with all the customers in the database, I am not computing the average of the period for each single customer.

2. Operation **b**:

   > For each terminal identify the possible fraudulent transactions. The fraudulent transactions are those whose import is higher than 20% of the maximal import of the transactions executed on the same terminal in the last month.

   Get all the fraudulent transactions that have an import higher than 20% the maximal import of the transactions executed on the same terminal in the last month.

   I am comparing the amount of the transactions executed on a terminal with **the maximal import of the transactions executed in the last month on the same terminal (+20%)**.

3. Operation **e**:

   > For each period of the day identifies the number of transactions that occurred in that period,
   > and the average number of fraudulent transactions.

   To identify the fraudulent transactions I will use the property `TX_FRAUD`.

#### Constraints:

- I have assumed that the transactions marked as fraudulent are really fraudulent under the definition of the term of operation **b**.
- I have assumed hat the values stored in the customer nodes about the average, the number of terminals, etc are correct.
- I have assumed that in the generated datasets there are no duplication
- I am using the identifiers provided from the datasets for comparing nodes when I need to find a specific one, but I haven't substituted the node identifier provided by Neo4J by default
- I have extended the data as stated in the operation **d** and I have choose to implements the extension in these ways:
  - For *period of the day* I am considering *morning* between 6 and 12, *afternoon* between 12 and 18, *evening* between 18 and 24 and *night* between 24 and 6
  - For *kind of products* I am using a generator based on a modulo 5 operations on `TRANSACTION_ID` to assign  the values
  - For *feeling of security* I am using the `rand` function provided by Neo4J
- Due to the choice that can be made on the degree of the operation **c** I am not storing the relationship *co-customer* into the database

## Generation scripts:

This script, cleaned out of imports and definition of functions, shows how I am generating the datasets.

I use a `dict` (`args_num`) to store the different amount of customers, terminals and days for each of the dataset required. Those amounts are going to satisfy the size constraint for them.

Then I exploit the functions given [by the website linked in the project]( fraud-
detection-handbook.github.io/fraud-detection-handbook/Chapter_3_GettingStarted/SimulatedDataset.html) to generate the dataframes that are going to be transformed into `csv` files. I haven't changed the initial data, but I have extended the period where the transactions happened to have relevant data to query (3 years, starting from *2018-04-01*).

Then I choose the directory where I am gonna store the `csv` files based on the dictionary key and exploit pandas function `to_csv` to save them.

The final step is passing through a function that clears the `csv` from the useless index and save the files. A step that can be substituted by the option in the `to_csv` function (`Index=False`).

```python
args_num:dict = {0: [1150, 100, 1095], 1: [2300, 200, 1095], 2: [6300, 800, 1095]}

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

    # saving customers
    customer_profiles_table.to_csv(DIR_OUTPUT + '/customers.csv', sep=';', encoding='utf-8')
    clean_csv(DIR_OUTPUT + '/customers.csv')
    # saving terminals   
    terminal_profiles_table.to_csv(DIR_OUTPUT + '/terminals.csv', sep=';', encoding='utf-8')
    clean_csv(DIR_OUTPUT + '/terminals.csv')
    # saving transactions:
    transactions_df.to_csv(DIR_OUTPUT + '/transactions.csv', sep=';', encoding='utf-8')
    clean_csv(DIR_OUTPUT + '/transactions.csv')
```

## Loading scripts:

To have an overview about the loading scripts I have included some pieces of the main one below.

The loading process involves two (or three) different techniques:

- For the customers and terminals I am using the native functionality of Neo4J, that let a user load nodes using a csv file hosted somewhere (gSheets publication in my case).
  So to make it works a user has to upload his `csv` files into Google cloud (or using the import function without the flag *convert*) to then publish them online as `csv`. 
- For the transactions there are two ways, but both of them involves the use of Python threads:
  - **Uploading row by row**, this is a really slow process that constraint the use to execute one creation operation at time. The use of threads mitigate a little because the user can parallelize the execution, but doesn't solve the problem.
    However the problem in this case was the period of three years in which the transactions can occur, it leads to a giant file and Google drive cannot support such a oversized file.
  - **Convert the creation into a cypher script**, this method permit to generate the creational operations and store them into a single file to then decide what to do.
    It also involves thread, but not this much as the first method, for each thread I create a temporary file, when all of them have finished the execution I merge all these files into a single one.

```python
csv_links = [
    'https://docs.google.com/customerCSV', 
    'https://docs.google.com/terminalCSV',  
]

conn.import_csv(csv_links[0], neo.FileType.CUSTOMERS)
conn.import_csv(csv_links[1], neo.FileType.TERMINALS)

def relationship_creator(rel_lines:list[str],i:int):
    # implementation
def relationship_saver(rel_lines:list[str],i:int):
    # implementation
def file_merger(file_extension:str):
    # implementation
  

def file_opener(file_name):
    with open(file_name, 'r') as file:
        try:
            lines = file.readlines()[1:200000]  # Discard the first line (header) and limit the number of lines due to the free tier!
            #Thread section:
            list_splitter = [i * 2000 for i in range(1, 101)]
            list_splitter.append(200000)
            threads = []
            for i in range(1, 101):
                # To save on the db really slow
                thread = threading.Thread(target=relationship_creator, args=(lines[list_splitter[i-1]:list_splitter[i]],i,))
                # To save into cql files using threads
                # thread = threading.Thread(target=relationship_saver, args=(lines[list_splitter[i-1]:list_splitter[i]],i,))
                
                threads.append(thread)
                thread.start()

            for thread in threads:
                thread.join()
            
            # file_merger('.cql')
        except:
            print('You have finished the free tier :/, maybe')

file_opener('../simulated-data-raw-50mb/transactions.csv')

conn.close()
```

The function `import_csv` is realized into the  `Neo` class (the class that holds all the operations and the connection to the db).

This is a simple function that create the statement that is going to be executed into the database and execute them in the static function named in the same way.

The only notable thing is the conversion that Neo4J offer natively into a large range of data types.

```python
    def import_csv(self, filepath:str, fileType: FileType):
        create_statement = ''
        
        match fileType:
            case FileType.CUSTOMERS:
                create_statement = """
                CREATE (:Customer {
                    CUSTOMER_ID: toInteger(line.CUSTOMER_ID),
                    x_customer_id: toFloat(line.x_customer_id),
                    y_customer_id: toFloat(line.y_customer_id),
                    mean_amount: toFloat(line.mean_amount),
                    std_amount: toFloat(line.std_amount),
                    mean_nb_tx_per_day: toFloat(line.mean_nb_tx_per_day),
                    available_terminals: line.available_terminals,
                    nb_terminals: toInteger(line.nb_terminals)
                })
                """
            case FileType.TERMINALS:
                create_statement = """
                CREATE (:Terminal {
                    TERMINAL_ID: toInteger(line.TERMINAL_ID),
                    x_terminal_id: toFloat(line.x_terminal_id),
                    y_terminal_id: toFloat(line.y_terminal_id)
                })
                """
            case _:
                raise ValueError('Invalid file type')
        with self.driver.session() as session:
            session.execute_write(self._import_csv, filepath, create_statement)
    @staticmethod
    def _import_csv(tx, filepath:str, create_statement:str):
        query = f"""
        LOAD CSV WITH HEADERS FROM '{filepath}' AS line
        {create_statement}
        """
        tx.run(query)

```

