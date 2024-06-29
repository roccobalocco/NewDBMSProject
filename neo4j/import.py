import neo
import os
import threading

conn = neo.Neo()

csv_links = [
    'https://docs.google.com/spreadsheets/d/e/2PACX-1vQBnQLFLdfWSFhd_to3HMDyJdQ4qupuzuWeftvJdMUBAthFqreUstKsHZdk0E-UnUTDyIClDrNAlO1l/pub?gid=1279874191&single=true&output=csv', # customers 
    'https://docs.google.com/spreadsheets/d/e/2PACX-1vR--1SpqbVMo4_18oRvVGtSgHBMnbQ_4i53QZrFUxYAVd9spQwe9m1jBs649mVG5_gav0q9PKZVrAdo/pub?gid=1649816939&single=true&output=csv', # terminals 
]

conn.import_csv(csv_links[0], neo.FileType.CUSTOMERS)
conn.import_csv(csv_links[1], neo.FileType.TERMINALS)

def relationship_creator(rel_lines:list[str],i:int):
    print("Starting thread {i}".format(i=i))
    for line in rel_lines:
        columns = line.split(';')
        statement = f"""
        MATCH (cc:Customer {{CUSTOMER_ID: '{columns[2]}'}}), (tt:Terminal {{TERMINAL_ID: '{columns[3]}'}})
        CREATE (cc) -[tr:Transaction {{
            TRANSACTION_ID: toInteger({columns[0]}),
            TX_DATETIME:  datetime({{epochMillis: apoc.date.parse('{columns[1]}', 'ms', 'yyyy-MM-dd HH:mm:ss')}}),
            TX_AMOUNT: toFloat({columns[4]}),
            TX_TIME_SECONDS: toInteger({columns[5]}),
            TX_TIME_DAYS: toInteger({columns[6]}),
            TX_FRAUD: toBoolean({columns[7]}),
            TX_FRAUD_SCENARIO: toInteger({columns[8]})
            }}]-> (tt);
        """
        # This is inside the for because appareantly the free tier has some issues concatenating create statements of this kind....
        # It takes a lot of time, really a lot. But my pc have also free time when I am sleeping
        conn.free_query(statement)
    print("Ending thread {i}".format(i=i))
        
def relationship_saver(rel_lines:list[str],i:int):
    print("Starting thread {i}".format(i=i))
    statements = ''  # Initialize the statements variable
    
    for line in rel_lines:
        columns = line.split(';')
        statements += f"""
        MATCH (cc:Customer {{CUSTOMER_ID: '{columns[2]}'}}), (tt:Terminal {{TERMINAL_ID: '{columns[3]}'}})
        CREATE (cc) -[tr:Transaction {{
            TRANSACTION_ID: toInteger({columns[0]}),
            TX_DATETIME:  datetime({{epochMillis: apoc.date.parse('{columns[1]}', 'ms', 'yyyy-MM-dd HH:mm:ss')}}),
            TX_AMOUNT: toFloat({columns[4]}),
            TX_TIME_SECONDS: toInteger({columns[5]}),
            TX_TIME_DAYS: toInteger({columns[6]}),
            TX_FRAUD: toBoolean({columns[7]}),
            TX_FRAUD_SCENARIO: toInteger({columns[8]})
            }}]-> (tt);
        """
        
    file_path = "../simulated-data-raw-50mb/transactionsThread" + str(i) + ".cql"
    # Open the file in write mode
    with open(file_path, 'w') as file:
        # Write content to the file
        file.write(statements)
    print("Ending thread {i}".format(i=i))
    
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
  

def file_opener(file_name):
    with open(file_name, 'r') as file:
        try:
            lines = file.readlines()[1:200000]  # Discard the first line (header) and limit the number of lines due to the free tier!
            create_statement = ''
            print('Starting to read the line of {file}, preparing {numRel} relationships'.format(file=file_name, numRel=len(lines)))

            #Thread section:
            list_splitter = [i * 2000 for i in range(1, 101)]
            list_splitter.append(200000)
            threads = []
            for i in range(1, 101):
                # To save on the db really slow
                thread = threading.Thread(target=relationship_creator, args=(lines[list_splitter[i-1]:list_splitter[i]],i,))
                # To save into cql files using threads
                #thread = threading.Thread(target=relationship_saver, args=(lines[list_splitter[i-1]:list_splitter[i]],i,))
                
                threads.append(thread)
                thread.start()

            for thread in threads:
                thread.join()
            
            # file_merger('.cql')
        except:
            print('You have finished the free tier :/, maybe')
# Do not try at home, the free tier will explode, only the first of my seven files will reach the node limit!
# Practically this was an attempt to use gsheets, i have divided my file into several files and tried to operate like this
# threads = []
# for i in range(1, 7):
#     file_path = '../simulated-data-raw-200mb/transactions' + str(i) + '.csv'
#     thread = threading.Thread(target=file_opener, args=(file_path,))
#     threads.append(thread)
#     thread.start()

# for thread in threads:
#     thread.join()

file_opener('../simulated-data-raw-50mb/transactions.csv')

conn.close()
