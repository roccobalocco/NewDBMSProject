import neo

conn = neo.Neo()

csv_links = [
    '', # customers 
    '', # terminals 
    '' # transactions 
]

for link in csv_links:
    conn.import_csv(link)

conn.close()

