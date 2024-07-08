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
conn = neo.Neo()

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

The function `relationship_saver` simply build the statement for the creation of a relationship in Neo4J and then exploit the `free_query` function to execute that statement.

The execution is sequential due to the impossibility of executing more statements at once outside the Neo4J console, however this is mitigated by the use of threads.	

```python
def relationship_creator(rel_lines:list[str],i:int):
    print("Starting thread {i}".format(i=i))
    for line in rel_lines:
        columns = line.split(',')
        statement = f"""
        MATCH (cc:Customer {{CUSTOMER_ID: {columns[3]}}}), (tt:Terminal {{TERMINAL_ID: {columns[4]}}})
        CREATE (cc) -[tr:Transaction {{
            TRANSACTION_ID: toInteger({columns[0]}),
            TX_DATETIME:  datetime({{epochMillis: apoc.date.parse('{columns[6]}', 'ms', 'yyyy-MM-dd HH:mm:ss')}}),
            TX_AMOUNT: toFloat({columns[5]}),
            TX_TIME_SECONDS: toInteger({columns[1]}),
            TX_TIME_DAYS: toInteger({columns[2]}),
            TX_FRAUD: toBoolean({columns[7]}),
            TX_FRAUD_SCENARIO: toInteger({columns[8]})
            }}]-> (tt);
        """
        # This is inside the for because appareantly the free tier has some issues concatenating create statements of this kind....
        # It takes a lot of time, really a lot. But my pc have also free time when I am sleeping
        conn.free_query(statement)
    print("Ending thread {i}".format(i=i))
```

## Operation scripts:

I included all the requested operations inside a class called `Neo`, that also includes the connection to the database, the disconnection and two method to execute unspecified statement inside Neo4J.

The connection exploit the environment variables to avoid hard-coding the sensible informations.

The `free_query` method execute an arbitrary statement to then put the results inside a DataFrame, this data structure is easily manipulable and also avoid us the creation of $n$ data structure to contain the results of the requested operations. While the `free_query_single` is a shortcut to the `single()` method provided by `neo4j` library, for the statement with a single result.

All the methods in this class have a documentation, not reported here, that can be easily transformed into `.md` file with [this simple script](https://github.com/roccobalocco/MD_Doc_Gen).

```python
class Neo:

    def __init__(self):
        try:
            print('Trying to open a connection with neo4j')
            self.driver = GraphDatabase.driver(os.environ['NEO_URI'], auth=(os.environ['NEO_USER'], os.environ['NEO_PSW']))
            print('Connection with neo4j opened')
        except:
            print('Could not open connection with neo4j')
            
    def close(self):
        print('Closing connection with neo4j')
        self.driver.close()
    
    def free_query(self, query)-> DataFrame:
        pandas_df = self.driver.execute_query(
            query,
            result_transformer_=neo4j.Result.to_df
        )
        return pandas_df 
        
    def free_query_single(self, query):
        with self.driver.session() as session:
            result = session.execute_write(self._free_query_single, query)
            return result

    def import_csv(self, filepath:str, fileType: FileType):
		# Implementation
		
    @staticmethod
    def _import_csv(tx, filepath:str, create_statement:str):
        query = f"""
        LOAD CSV WITH HEADERS FROM '{filepath}' AS line
        {create_statement}
        """
        print(f"Executing query: {query}")  # Print the query for debugging
        tx.run(query)
        
    @staticmethod
    def _free_query(tx, query) -> DataFrame:
        pandas_df = tx.execute_query(
            query,
            result_transformer_= neo4j.Result.to_df
        )
        return pandas_df
        
    @staticmethod
    def _free_query_single(tx, query):
        result = tx.run(query)
        return result.single()
   
    # Operation a:
    def get_customer_under_average(self, dt_start: datetime, dt_end: datetime)-> DataFrame:
		# Implementation
    
    def get_period_average_spending_amounts(self, dt_start:date, dt_end:date)-> float:
		# Implementation
    
    def get_period_average_spending_frequency(self, dt_start:date, dt_end:date)-> float:
		# Implementation
    
    # Operation b:
    def get_fraudolent_transactions(self, terminal_id:str, dt_start:date, dt_end:date)-> DataFrame:
    	# Implementation
    
    def get_terminal_max_import_last_month(self, terminal_id:str, dt_start:date, dt_end:date)-> float:
    	# Implementation
        
	# Operation c:
    def get_co_customer_relationships_of_degree_k(self, u:int, k:int)-> DataFrame:
    	# Implementation
        
    # Operation d:
    def extend_neo(self)-> None:
    	# Implementation
    
    def extend_neo_with_period(self)-> None:
    	# Implementation
    
    def extend_neo_with_kind_of_product(self)-> None:
    	# Implementation
    
    def extend_neo_with_feeling_of_security(self)-> None:
    	# Implementation
    
    def connect_buying_friends(self)-> None:
    	# Implementation
        
	# Operation e:
    
    def get_transactions_per_period(self, dt_start: date, dt_end: date)-> DataFrame:
    	# Implementation
    
    def get_fraudolent_transactions_per_period(self, dt_start: date, dt_end: date)-> DataFrame:
    	# Implementation
    
```

### Operation a:

> For each customer checks that the spending frequency and the spending amounts of the last month is under the usual spending frequency and the spending amounts for the same period.

To realize this operation I exploit two side methods to retrieve the average spending amount and the average spending frequency of a period. The entire operation was extended to an arbitrary period of time instead of the last month.

Once calculated the two averages I use them into the main query to retrieve all the Customers that are under the average of spending amount and average of spending frequency.

Here can be seen the utility provided by the `free_query_single`, that exploit the `.single()` method. Once the method return the result we can access them with the string decided in the return statement of our query.

```python
def get_customer_under_average(self, dt_start: datetime, dt_end: datetime)-> DataFrame:
    avg_spending_amount = self.get_period_average_spending_amounts(dt_start, dt_end)
    avg_spending_frequency = self.get_period_average_spending_frequency(dt_start, dt_end)
    print('avg_spending_amount:', avg_spending_amount, 'avg_spending_frequency:', avg_spending_frequency)
    query = f"""
    MATCH (c:Customer)  -[t:Transaction]-> (:Terminal)
    WHERE datetime(t.TX_DATETIME) >= '{dt_start}' 
    AND 
    datetime(t.TX_DATETIME) <= '{dt_end}'
    WITH c, AVG(t.TX_AMOUNT) as avg_amount, COUNT(t) as nb_tx
    WHERE avg_amount < {avg_spending_amount} AND nb_tx < {avg_spending_frequency}
    RETURN collect(c) as customers
    """

    customers = self.free_query(query)
    return customers
def get_period_average_spending_amounts(self, dt_start:date, dt_end:date)-> float:
    query = f"""
    MATCH ()-[t:Transaction]->(:Terminal)
    WHERE 
        datetime(t.TX_DATETIME).month >= {dt_start.month} 
        AND 
        datetime(t.TX_DATETIME).day >= {dt_start.day}
        AND 
        datetime(t.TX_DATETIME).month <= {dt_end.month} 
        AND
        datetime(t.TX_DATETIME).day <= {dt_end.day}
    RETURN AVG(t.TX_AMOUNT) as avg_spending_amount
    """

    avg_spending_amount = self.free_query_single(query)
    if (avg_spending_amount is None):
        return 0.
    return float(avg_spending_amount["avg_spending_amount"])  # type: ignore
def get_period_average_spending_frequency(self, dt_start:date, dt_end:date)-> float:
    query = f"""
    MATCH (c:Customer)-[t:Transaction]->(:Terminal)
    WHERE
    datetime(t.TX_DATETIME).month >= {dt_start.month}
    AND 
    datetime(t.TX_DATETIME).day >= {dt_start.day}
    AND 
    datetime(t.TX_DATETIME).month <= {dt_end.month}
    AND 
    datetime(t.TX_DATETIME).day <= {dt_end.day}
    WITH COUNT(t) as transaction_number, COUNT(DISTINCT c) as customer_number
    RETURN transaction_number*1.0/customer_number as avg_spending_frequency
    """
    avg_spending_frequency  = self.free_query_single(query)

    if (avg_spending_frequency is None):
        return 0.
    return float(avg_spending_frequen
```

### Operation b:

> For each terminal identify the possible fraudulent transactions. 
>
> The fraudulent transactions are those whose import is higher than 20% of the maximal import of the transactions executed on the same terminal in the last month.

For this operation I have realized a support method called `get_terminal_max_import_last_month` who permits to retrieve the maximum import on a given terminal in a given period (also here I have extended the last month request to an arbitrary period).

Once retrieved the maximal import of the period I compute the 120% of it, to then use it inside the main query, that identify all the fraudulent transitions based on the comparison of this amount with each amount of the transactions.

```python
def get_fraudolent_transactions(self, terminal_id:str, dt_start:date, dt_end:date)-> DataFrame:
    maximal_import = self.get_terminal_max_import_last_month(terminal_id, dt_start, dt_end)
    maximal_import_20 = maximal_import + (maximal_import * 0.2)
    query = f"""
    MATCH (:Terminal {{TERMINAL_ID: {terminal_id}}}) -[t:Transaction]- ()
    WHERE t.TX_AMOUNT >= {maximal_import_20}
    RETURN collect(t) as fraudolent_transactions
    """

    fraudolent_transactions = self.free_query(query)
    return fraudolent_transactions
def get_terminal_max_import_last_month(self, terminal_id:str, dt_start:date, dt_end:date)-> float:
    query = f"""
    MATCH (t:Terminal {{TERMINAL_ID: {terminal_id}}}) <-[tr:Transaction]- (:Customer)
    WHERE 
    datetime(tr.TX_DATETIME) >= datetime({{epochMillis: apoc.date.parse('{dt_start}', 'ms', 'yyyy-MM-dd HH:mm:ss')}})
    AND 
    datetime(tr.TX_DATETIME) <= datetime({{epochMillis: apoc.date.parse('{dt_end}', 'ms', 'yyyy-MM-dd HH:mm:ss')}})
    RETURN MAX(tr.TX_AMOUNT) as max_import
    """

    maximal_import = self.free_query_single(query)
    if (maximal_import is None):
        return 0.
    return float(maximal_import["max_import"])
```
### Operation c:

> Retrieve the *co-customer relationships* of degree $k$

This simple method embeds the request, it takes the user id (`u`) and the degree of the relationship (`k`) in input to then return a DataFrame with the collection of distinct customers that are categorized as *co-customer*  of degree $k$.  

```python
def get_co_customer_relationships_of_degree_k(self, u:int, k:int)-> DataFrame:
    query = f"""
    MATCH (c:Customer {{CUSTOMER_ID: {u}}}) -[:Transaction*{k}]- (co:Customer)
    WHERE c <> co
    RETURN collect(DISTINCT co) as co_customers
    """

    co_customers = self.free_query(query)
    return co_customers
```
### Operation d:

This operation is divided in four methods, called all at once with `extend_neo` that are executed sequentially.

> The period of the day {morning, afternoon, evening, night} in which the transaction has been executed.

`extend_neo_with_period` simply assigns a *period of time* by comparing the hour of execution of the transition.

> The kind of products that have been bought through the transaction {high-tech, food, clothing, consumable, other}

`extend_neo_with_kind_of_product` simply assign a type of product based on the modulo five of `TERMINAL_ID`.

> The feeling of security expressed by the user. This is an integer value between 1 and 5 expressed by the user when conclude the transaction.

`extend_neo_with_feeling_of_security` simply assign a value for feeling of security based on the `rand()` funciton provided by Neo4K.

> Customers that make more than three transactions from the same terminal expressing a similar average feeling of security should be connected as “buying_friends”. Therefore also this kind of relationship should be explicitly stored in the NOSQL database and can be queried. Note, two average feelings of security are considered similar when their difference is lower than 1.

`connect_buying_friends` is a simple method that connect Customers with the *buying_friends* relationship if they both have at least four transactions on the same terminal that express a similar average of feeling of security (the difference between the averages should be lower than one).

The relationships are really written in the database to let a user query them later.

```python
def extend_neo(self)-> None:
    self.extend_neo_with_period()
    self.extend_neo_with_kind_of_product()
    self.extend_neo_with_feeling_of_security()
    self.connect_buying_friends()

def extend_neo_with_period(self)-> None:
    query = """
    MATCH ()-[t:Transaction]-()
    WITH distinct t as single_t
    SET single_t.PERIOD_OF_DAY = CASE
        WHEN single_t.TX_DATETIME.hour >= 6 AND single_t.TX_DATETIME.hour < 12 THEN 'morning'
        WHEN single_t.TX_DATETIME.hour >= 18 AND single_t.TX_DATETIME.hour < 24 THEN 'evening'
        WHEN single_t.TX_DATETIME.hour >= 12 AND single_t.TX_DATETIME.hour < 18 THEN 'afternoon'
        ELSE 'night'
    END
    """
    
    self.free_query(query)

def extend_neo_with_kind_of_product(self)-> None:
    query = """
    MATCH ()-[tt:Transaction]-()
    WITH distinct tt as t
    SET t.KIND_OF_PRODUCT = CASE
        WHEN t.TRANSACTION_ID % 5  = 0 THEN 'high-tech'
        WHEN t.TRANSACTION_ID % 5  = 1 THEN 'food'
        WHEN t.TRANSACTION_ID % 5  = 2 THEN 'clothing'
        WHEN t.TRANSACTION_ID % 5  = 3 THEN 'consumable'
        ELSE 'other'
    END
    return t.KIND_OF_PRODUCT
    """
    
    self.free_query(query)
    
def extend_neo_with_feeling_of_security(self)-> None:
    query = """
    MATCH ()-[tt:Transaction]-()
    WITH distinct tt as t
    SET t.FEELING_OF_SECURITY = toInteger(rand() * 5) + 1
    """
    
    self.free_query(query)

def connect_buying_friends(self)-> None:
    query = """
    MATCH (c1:Customer)-[tc1:Transaction]->(terminal:Terminal)<-[tc2:Transaction]-(c2:Customer)
    WHERE c1.CUSTOMER_ID <> c2.CUSTOMER_ID 
    WITH terminal.TERMINAL_ID AS terminal_id, count(distinct tc1) as tnc1_num, c1, count(distinct tc2) as tnc2_num, c2, 
        AVG(tc1.FEELING_OF_SECURITY) AS tc1_avg_fos, AVG(tc2.FEELING_OF_SECURITY) AS tc2_avg_fos
    WHERE tnc1_num > 3 and tnc2_num > 3 AND abs(tc1_avg_fos - tc2_avg_fos) < 1
    CREATE (c1)-[:BUYING_FRIEND]->(c2)
    """
    
    self.free_query(query)
```

### Operation e:

> For each period of the day identifies the number of transactions that occurred in that period,
> and the average number of fraudulent transactions.

I have divided this operation in two distinct methods and I also extend the method to let a user indicate a period of time.

`get_transactions_per_period`  retrieve all the transactions happened in a period divided into the different values of period of the day in a DataFrame. 

`transactions_per_period` retrieve all the fraudulent transactions happened in a period divided into the different values of period of the day. The recognition of a fraudulent transaction exploit, here, the `TX_FRAUD` property, to avoid the computation of the operation **b** that would cost a lot of performance and can depend on the considered period.

```python
def get_transactions_per_period(self, dt_start: date, dt_end: date)-> DataFrame:
    query = f"""
    MATCH ()-[t:Transaction]-()
    WHERE 
    datetime(t.TX_DATETIME) >= datetime({{epochMillis: apoc.date.parse('{dt_start}', 'ms', 'yyyy-MM-dd HH:mm:ss')}})
    AND 
    datetime(t.TX_DATETIME) <= datetime({{epochMillis: apoc.date.parse('{dt_end}', 'ms', 'yyyy-MM-dd HH:mm:ss')}})
    RETURN t.PERIOD_OF_DAY, collect(t) as transactions_per_period
    """

    transactions_per_period = self.free_query(query)
    return transactions_per_period
def get_fraudolent_transactions_per_period(self, dt_start: date, dt_end: date)-> DataFrame:
    query = f"""
    MATCH ()-[t:Transaction]-()
    WHERE 
    datetime(t.TX_DATETIME) >= datetime({{epochMillis: apoc.date.parse('{dt_start}', 'ms', 'yyyy-MM-dd HH:mm:ss')}}) 
    AND 
    datetime(t.TX_DATETIME) <= datetime({{epochMillis: apoc.date.parse('{dt_end}', 'ms', 'yyyy-MM-dd HH:mm:ss')}})
    AND t.TX_FRAUD = true
    WITH t.PERIOD_OF_DAY as period_of_day, collect(t) as fraudulent_transactions, count(t) as tn_per_period_of_the_day
    RETURN period_of_day, fraudulent_transactions, avg(tn_per_period_of_the_day) as average_number_of_transactions
    """

    result = self.free_query(query)
    return result
```

## Notes:

- All the code showed in this document can be see in [this repository](https://github.com/roccobalocco/NewDBMSProject).
- For the realization of the project I used the free tier of Neo4J.
- For the diagrams I used [draw.io](https://www.drawio.com/) and [Star UML](https://www.staruml.io).
- I tried to use as many types as possible even if I am using python, to have better readability and code awareness (or maybe, I do not like untyped languages)
