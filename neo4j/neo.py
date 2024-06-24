from neo4j import GraphDatabase
import os
from enum import Enum

class FileType(Enum):
    CUSTOMERS = 0
    TERMINALS = 1
    TRANSACTIONS = 2

class Neo:

    def __init__(self):
        try:
            print('Trying to open a connection with neo4j')
            self.driver = GraphDatabase.driver(os.environ['NEO-URI'], auth=(os.environ['NEO-USER'], os.environ['NEO-PSW']))
            print('Connection with neo4j opened')
        except:
            print('Could not open connection with neo4j')
            
    def close(self):
        print('Closing connection with neo4j')
        self.driver.close()

    def free_query(self, query):
        with self.driver.session() as session:
            session.execute_write(self._free_query, query)

    def import_csv(self, filepath:str, fileType: FileType):
        create_statement = ''
        print('filetype:', FileType(fileType), fileType)
        match fileType:
            case FileType.CUSTOMERS:
                create_statement = """
                CREATE (:Customer {
                    CUSTOMER_ID: line.CUSTOMER_ID,
                    x_customer_id: line.x_customer_id,
                    y_customer_id: line.y_customer_id,
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
                    TERMINAL_ID: line.TERMINAL_ID,
                    x_terminal_id: line.x_terminal_id,
                    y_terminal_id: line.y_terminal_id
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
        print(f"Executing query: {query}")  # Print the query for debugging
        tx.run(query)
        
        
    @staticmethod
    def _free_query(tx, query):
        tx.run(query)

if __name__ == "__main__":
    greeter = Neo()
    greeter.close()