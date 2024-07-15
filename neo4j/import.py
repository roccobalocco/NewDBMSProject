import neo
import os
import threading

csv_links = [
    'https://docs.google.com/spreadsheets/d/e/2PACX-1vRO0xjWbbTnq8sHY-pkKeWI7W1BfXab-9-qgw2WAqDtKtqJK2fffd6qbEjsKg-0Kj8smec7jo6RXwgv/pub?gid=1920686016&single=true&output=csv', # customers 
    'https://docs.google.com/spreadsheets/d/e/2PACX-1vRmNHkcX5rYI_lx8LGRK-_d6pm0e5pBgtn7VwWfgq2k2yS3767iD-Zq-_jX_2zrVF6YvWcPc6mHEIjj/pub?gid=641078959&single=true&output=csv', # terminals 
]

conn = neo.Neo()

# conn.import_csv(csv_links[0], neo.FileType.CUSTOMERS)
# conn.import_csv(csv_links[1], neo.FileType.TERMINALS)

def relationship_creator(rel_lines:list[str],i:int):
    print("Starting thread {i}".format(i=i))
    for line in rel_lines:
        columns = line.split(';')
        statement = f"""
        MATCH (cc:Customer {{CUSTOMER_ID: {columns[3]}}}), (tt:Terminal {{TERMINAL_ID: {columns[4]}}})
        CREATE (cc) -[tr:Transaction {{
            TRANSACTION_ID: toInteger({columns[0]}),
            TX_DATETIME:  datetime({{epochMillis: apoc.date.parse('{columns[6]}', 'ms', 'yyyy-MM-dd HH:mm:ss')}}),
            TX_AMOUNT: toFloat({columns[5]}),
            TX_TIME_SECONDS: toInteger({columns[1]}),
            TX_TIME_DAYS: toInteger({columns[2]}),
            TX_FRAUD: toBoolean({columns[7]}),
            TX_FRAUD_SCENARIO: toInteger({columns[8]})}}]-> (tt);
        """
        # This is inside the for because appareantly the free tier has some issues concatenating create statements of this kind....
        # It takes a lot of time, really a lot. But my pc have also free time when I am sleeping
        # Define metadata if any (this is optional)
        metadata = {
            "purpose": "Importing transaction from a list into the database"
        }

        # Create the Query object
        neo4j_query = Query(text=statement, metadata=metadata) #type: ignore

        conn.free_query_single(neo4j_query)
    print("Ending thread {i}".format(i=i))
        
def relationship_saver(rel_lines:list[str],i:int):
    print("Starting thread {i}".format(i=i))
    statements = ''  # Initialize the statements variable
    
    for line in rel_lines:
        columns = line.split(';')
        statements += f"""
        MATCH (cc:Customer {{CUSTOMER_ID: {columns[3]}}}), (tt:Terminal {{TERMINAL_ID: {columns[4]}}})
        CREATE (cc) -[tr:Transaction {{
            TRANSACTION_ID: toInteger({columns[0]}),
            TX_DATETIME:  datetime({{epochMillis: apoc.date.parse('{columns[6]}', 'ms', 'yyyy-MM-dd HH:mm:ss')}}),
            TX_AMOUNT: toFloat({columns[5]}),
            TX_TIME_SECONDS: toInteger({columns[1]}),
            TX_TIME_DAYS: toInteger({columns[2]}),
            TX_FRAUD: toBoolean({columns[7]}),
            TX_FRAUD_SCENARIO: toInteger({columns[8]})
            }}]-> (tt)
        RETURN 'ok';
        """
        
    file_path = f"../simulated-data-raw-50mb/transactionsThread{i}.cql"
    # Open the file in write mode
    with open(file_path, 'w') as file:
        # Write content to the file
        file.write(statements)
        file.close()
    print("Ending thread {i}".format(i=i))

def run_many(path:str):
    print(f"Starting to run the file {path}")
    with open(path, 'r') as file:
        lines = file.read()
        query = f'''
            CALL apoc.cypher.runMany(
            "{lines}",
            {{}}
        );'''
        # Define metadata if any (this is optional)
        metadata = {
            "purpose": "Importing transactions from a file into the database."
        }

        # Create the Query object
        neo4j_query = Query(text=query, metadata=metadata) #type: ignore

        conn.free_query(neo4j_query)
        file.close()

    print(f"Ending to run the file {path}")
    os.remove(path)

def file_merger(file_extension:str):
    # List all the files in the directory
    files = os.listdir('../simulated-data-raw-50mb/')
    # Initialize the statements variable
    statements = ''
    # Loop through the files
    for file in files:
        # Check if the file has the required extension
        if file.endswith(file_extension):
            # Open the file in read mode
            with open('../simulated-data-raw-50mb/' + file, 'r') as f:
                # Read the content of the file
                content = f.read()
                # Append the content to the statements variable
                statements += content
            os.remove('../simulated-data-raw-50mb/' + file)
    # Open the file in write mode
    with open('../simulated-data-raw-50mb/transactions.cql', 'w') as f:
        # Write the content to the file
        f.write(statements)
        f.close()
  

def file_opener(file_name):
    with open(file_name, 'r') as file:
        try:
            lines = file.readlines()[1:25810]  # Discard the first line (header) and limit the number of lines due to the free tier!
            print('Starting to read the line of {file}, preparing {numRel} relationships'.format(file=file_name, numRel=len(lines)))

            #Thread section:
            list_splitter = [i * 1000 for i in range(1, 24)]
            list_splitter.append(25810)
            threads = []
            for i in range(1, 24):
                # To save on the db really slow
                #thread = threading.Thread(target=relationship_creator, args=(lines[list_splitter[i-1]:list_splitter[i]],i,))
                # To save into cql files using threads
                thread = threading.Thread(target=relationship_saver, args=(lines[list_splitter[i-1]:list_splitter[i]],i,))
                
                threads.append(thread)
                thread.start()

            for thread in threads:
                thread.join()
            
            threads = []
            # To merge all the generated files into a single one
            # file_merger('.cql')

            # To execute the multiple statements from the file in a single query with a number of threads that reflects the number of files
            threads = []
            for i in range(1, 24):
                arg = f'../simulated-data-raw-50mb/transactionsThread{i}.cql'
                thread = threading.Thread(target=run_many, args=(arg,))
                
                threads.append(thread)
                thread.start()

            for thread in threads:
                thread.join()
        except Exception as e:
            print(f'You have finished the free tier :/, maybe - \n {e}')

file_opener('../simulated-data-raw-50mb/transactions.csv')

conn.close()
