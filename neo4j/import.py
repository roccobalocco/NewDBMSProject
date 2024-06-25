import neo
import os
import threading

conn = neo.Neo()

csv_links = [
    'https://docs.google.com/spreadsheets/d/e/2PACX-1vQL2bzUZly1hPp8WJx9RNAWOz-wrK7VfZ3I_mC72T-Ui7gUeHir956EYAM6JH_2-iRq1S-U4_W_-pwZ/pub?gid=2006922834&single=true&output=csv', # customers 
    'https://docs.google.com/spreadsheets/d/e/2PACX-1vTaZNCMx2Jl4yYOqfLwAHUf2LXCTGKN6MLMufpNNPw-1BEBizFvh22c8t_LkmeIy6dtFE-Nj38jJSHz/pub?gid=2106011748&single=true&output=csv', # terminals 
]

#conn.import_csv(csv_links[0], neo.FileType.CUSTOMERS)
#conn.import_csv(csv_links[1], neo.FileType.TERMINALS)

def relationship_creator(rel_lines:list[str],i:int):
    print("Starting thread {i}".format(i=i))
    matches = ''
    creates = ''
    for line in rel_lines:
        columns = line.split(',')
        statement = f"""
        MATCH (cc:Customer {{CUSTOMER_ID: '{columns[3]}'}}), (tt:Terminal {{TERMINAL_ID: '{columns[4]}'}})
        CREATE (cc) -[tr:Transaction {{
            TRANSACTION_ID: '{columns[1]}',
            TX_DATETIME: '{columns[2]}',
            TX_AMOUNT: toFloat({columns[5]}),
            TX_TIME_SECONDS: toInteger({columns[6]}),
            TX_TIME_DAYS: toInteger({columns[7]}),
            TX_FRAUD: '{columns[8]}',
            TX_FRAUD_SCENARIO: '{columns[9]}'
            }}]-> (tt);
        """
        # This is inside the for because appareantly the free tier has some issues concatenating create statements of this kind....
        # It takes a lot of time, really a lot. But my pc have also free time when I am sleeping
        conn.free_query(statement)
    print("Ending thread {i}".format(i=i))
        

def file_opener(file_name):
    with open(file_name, 'r') as file:
        try:
            lines = file.readlines()[1:200000]  # Discard the first line (header) and limit the number of lines due to the free tier!
            create_statement = ''
            print('Starting to read the line of {file}, preparing {numRel} relationships'.format(file=file_name, numRel=len(lines)))
            list_splitter = [i * 4000 for i in range(1, 51)]
            list_splitter.append(200000)
            threads = []
            for i in range(1, 51):
                thread = threading.Thread(target=relationship_creator, args=(lines[list_splitter[i-1]:list_splitter[i]],i,))
                threads.append(thread)
                thread.start()

            for thread in threads:
                thread.join()
        except:
            print('You have finished the free tier :/, maybe')
# Do not try at home, the free tier will explode, only the first of my seven files will reach the node limit!
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
