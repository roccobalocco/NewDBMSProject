import neo
import os
import neo4j
import threading
import time
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.poolmanager import PoolManager
import ssl

## This is a workaround to avoid an SSL error
class MyAdapter(HTTPAdapter):
    def init_poolmanager(self, connections, maxsize, block=False):
        self.poolmanager = PoolManager(num_pools=connections,
                                       maxsize=maxsize,
                                       block=block,
                                       ssl_version=ssl.PROTOCOL_TLSv1)
import requests
s = requests.Session()
s.mount('https://', MyAdapter())
#End of workaround for SSL error

csv_links = [
    #50mb
    #'https://docs.google.com/spreadsheets/d/e/2PACX-1vQBnQLFLdfWSFhd_to3HMDyJdQ4qupuzuWeftvJdMUBAthFqreUstKsHZdk0E-UnUTDyIClDrNAlO1l/pub?gid=1035716438&single=true&output=csv', # customers 
    #'https://docs.google.com/spreadsheets/d/e/2PACX-1vR--1SpqbVMo4_18oRvVGtSgHBMnbQ_4i53QZrFUxYAVd9spQwe9m1jBs649mVG5_gav0q9PKZVrAdo/pub?gid=1246437135&single=true&output=csv', # terminals 
    #100mb
    # 'https://docs.google.com/spreadsheets/d/e/2PACX-1vRZl6fxBltr9ATsm0R_u5Q6IZ-z5K2jH7BQeKVklEndKT5dl8c5TuoQrdq0E_1A2SpNMGJGSPdQKIrr/pub?gid=1349588355&single=true&output=csv',
    # 'https://docs.google.com/spreadsheets/d/e/2PACX-1vRbSDGZ35kaQ2codyQcCokBi4QEAIIVHmm82UFT-P61PIUXXnuOSsfRnbnBu62qE_79lz3aYxOuw53G/pub?gid=1574007303&single=true&output=csv',
    #200mb
    'https://docs.google.com/spreadsheets/d/e/2PACX-1vR1az00pghQCKTXRueW3GJUIMl-topQzr_OkHDeOok3lCyeuFyO6mNL0tTfq0QVDLx58Z0ntrGlBCeL/pub?gid=64778896&single=true&output=csv',
    'https://docs.google.com/spreadsheets/d/e/2PACX-1vQWW9O9GvCwcJGDeaIJlZ4wRaGnBIFH5yMRlQffcUjFKeyoJbXhHEofOhThGXUgNwpdikLw448XnRXW/pub?gid=1543043513&single=true&output=csv'
]

conn = neo.Neo()

# conn.import_csv(csv_links[0], neo.FileType.CUSTOMERS)
# conn.import_csv(csv_links[1], neo.FileType.TERMINALS)

# old function to import the csv files line per line
def relationship_creator(rel_lines:list[str],i:int):
    print("Starting thread {i}".format(i=i))
    for line in rel_lines:
        columns = line.split(';') # it may be a coma or a semicolon, depending on the generated csv file
        statement = f"""
        MATCH (cc:Customer {{CUSTOMER_ID: {columns[2]}}}), (tt:Terminal {{TERMINAL_ID: {columns[3]}}})
        CREATE (cc) -[tr:Transaction {{
            TRANSACTION_ID: toInteger({columns[0]}),
            TX_DATETIME:  datetime({{epochMillis: apoc.date.parse('{columns[1]}', 'ms', 'yyyy-MM-dd HH:mm:ss')}}),
            TX_AMOUNT: toFloat({columns[4]}),
            TX_TIME_SECONDS: toInteger({columns[5]}),
            TX_TIME_DAYS: toInteger({columns[6]}),
            TX_FRAUD: toBoolean({columns[7]}),
            TX_FRAUD_SCENARIO: toInteger({columns[8]})}}]-> (tt) ;
        """
        # It takes a lot of time, really a lot. But my pc have also free time when I am sleeping
        # Define metadata if any (this is optional)
        metadata = {
            "purpose": "Importing transaction from a list into the database"
        }

        # Create the Query object
        neo4j_query = neo4j.Query(text=statement, metadata=metadata) #type: ignore

        conn.free_query_single(neo4j_query)
    print("Ending thread {i}".format(i=i))
        
# function to save n lines in the a file
def relationship_saver(rel_lines:list[str],i:int):
    print("Starting thread {i}".format(i=i))
    statements = ''  # Initialize the statements variable
    
    for line in rel_lines:
        columns = line.split(',')
        statements += f"""MERGE (cc:Customer {{CUSTOMER_ID: {columns[2]}}}) MERGE (tt:Terminal {{TERMINAL_ID: {columns[3]}}}) MERGE (cc)-[tr:Transaction {{TRANSACTION_ID: {columns[0]}}}]->(tt) ON CREATE SET tr.TRANSACTION_ID = {columns[0]}, tr.TX_DATETIME = datetime({{epochMillis: apoc.date.parse('{columns[1]}', 'ms', 'yyyy-MM-dd HH:mm:ss')}}), tr.TX_AMOUNT = toFloat({columns[4]}), tr.TX_TIME_SECONDS = {columns[5]}, tr.TX_TIME_DAYS = {columns[6]},tr.TX_FRAUD = toBoolean({columns[7]}),tr.TX_FRAUD_SCENARIO = {columns[8]} RETURN 'ok';
        """
    file_path = f"../simulated-data-raw-200mb/transactionsThread{i}.cql"
    # Open the file in write mode
    with open(file_path, 'w') as file:
        # Write content to the file
        file.write(statements)
        file.close()
    print("Ending thread {i}".format(i=i))

# function to execute the multiple statements from the file in a single query exploiting APOC
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
        neo4j_query = neo4j.Query(text=query, metadata=metadata) #type: ignore

        res = conn.free_query(neo4j_query, directToDF=True)
        file.close()

    print(f"Ending to run the file {path}")
    os.remove(path)

# Function that merges multiple files into one, if needed
def file_merger(file_extension:str):
    # List all the files in the directory
    files = os.listdir('../simulated-data-raw-200mb/')
    # Initialize the statements variable
    statements = ''
    # Loop through the files
    for file in files:
        # Check if the file has the required extension
        if file.endswith(file_extension):
            # Open the file in read mode
            with open('../simulated-data-raw-200mb/' + file, 'r') as f:
                # Read the content of the file
                content = f.read()
                # Append the content to the statements variable
                statements += content
            os.remove('../simulated-data-raw-200mb/' + file)
    # Open the file in write mode
    with open('../simulated-data-raw-200mb/transactions.cql', 'w') as f:
        # Write the content to the file
        f.write(statements) 
        f.close()

def file_opener(file_name):
    """Opens a file and processes its contents in different ways depending on the function called inside it.
    It can exploit the @relationship_creator, @relationship_saver, @run_many, and @file_merger functions.

    Args:
        file_name (str): The name of the file to be opened (the relative path usually).

    Raises:
        Exception: If an error occurs while processing the file.

    Returns:
        None
    """
    with open(file_name, 'r') as file:
        try:
            #lines = file.readlines()[1:13183738]  # Discard the first line (header) and limit the number of lines
            threadNum = 2000
            lines = file.readlines()[1:]
            lineCount = len(lines)
            rowPerThread = lineCount // threadNum
            print('Starting to read the line of {file}, preparing {numRel} relationships'.format(file=file_name, numRel=lineCount))

            # Thread section:
            list_splitter = [i * rowPerThread for i in range(1, threadNum)]
            threads = []

            for i in range (1, threadNum):
                # To save on the db really slow
                # thread = threading.Thread(target=relationship_creator, args=(lines[list_splitter[i-1]:list_splitter[i]],i,))
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
            for i in range (781, threadNum):
                arg = f'../simulated-data-raw-200mb/transactionsThread{i}.cql'
                thread = threading.Thread(target=run_many, args=(arg,))
                threads.append(thread)
                thread.start()
                #time.sleep(1) # for 100mb
                time.sleep(25) # for 200mb due to heapsize or free tier limitations (if you are using it)

            for thread in threads:
                thread.join()
        except Exception as e:
            print(f'Something wrong happened - \n {e}')

file_opener('../simulated-data-raw-200mb/transactions.csv')

conn.close()
