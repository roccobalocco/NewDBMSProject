import neo
import os
import threading

conn = neo.Neo()

csv_links = [
    'https://docs.google.com/spreadsheets/d/e/2PACX-1vRHRb0m35byJhbv_Q_50Eg9rFc32pXSTWGkr22LWZkUO3FecP1yd0R7RI6JbTbcaYnJVhFII8iqpXcQ/pub?gid=1503518093&single=true&output=csv', # customers 
    'https://docs.google.com/spreadsheets/d/e/2PACX-1vTv7gcQLasDnekrUCghc71U_qhJ8LldrBZ7BgUgR8djly9NGLNp3qoLITMm1VfnyxfWeL8vngXsj8ue/pub?gid=766734284&single=true&output=csv', # terminals 
]

conn.import_csv(csv_links[0], neo.FileType.CUSTOMERS)
conn.import_csv(csv_links[1], neo.FileType.TERMINALS)

def file_opener(file_name):
    with open(file_name, 'r') as file:
        lines = file.readlines()[1:]  # Discard the first line (header)
        create_statement = ''
        for line in lines:
            columns = line.split(',')
            create_statement += f"""
            CREATE (:Transaction {{
            TRANSACTION_ID: '{columns[0]}',
            TX_DATETIME: '{columns[1]}',
            CUSTOMER_ID: '{columns[2]}',
            TERMINAL_ID: '{columns[3]}',
            TX_AMOUNT: toFloat({columns[4]}),
            TX_TIME_SECONDS: toInteger({columns[5]}),
            TX_TIME_DAYS: toInteger({columns[6]}),
            TX_FRAUD: '{columns[7]}',
            TX_FRAUD_SCENARIO: '{columns[8]}'
            }})
            """
        conn.free_query(create_statement)

# Do not try at home, the free tier will explode, only the first of my seven files will reach the node limit!
threads = []
for i in range(1, 7):
    file_path = '../simulated-data-raw-200mb/transactions' + str(i) + '.csv'
    thread = threading.Thread(target=file_opener, args=(file_path,))
    threads.append(thread)
    thread.start()

for thread in threads:
    thread.join()


conn.close()
