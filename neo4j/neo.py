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

    def print_greeting(self, message):
        with self.driver.session() as session:
            greeting = session.execute_write(self._create_and_return_greeting, message)
            print(greeting)

    def import_csv(self, filepath:str):
        with self.driver.session() as session:
            session.execute_write(self._import_csv, filepath)

    @staticmethod
    def _import_csv(tx, filepath:str, case: FileType):
        query = f"""
        LOAD CSV WITH HEADERS FROM '{filepath}' AS line
        CREATE (:YourNodeLabel {{property1: line.column1, property2: line.column2}})
        """
        tx.run(query)
        
        
    @staticmethod
    def _create_and_return_greeting(tx, message):
        result = tx.run("CREATE (a:Greeting) "
                        "SET a.message = $message "
                        "RETURN a.message + ', from node ' + id(a)", message=message)
        return result.single()[0]

# customers -> https://drive.google.com/file/d/10aYf0TUOH0kZs45rx1p-mJe6gQnTmbDG/view?usp=sharing
# terminals -> https://drive.google.com/file/d/18yd8H0Z1IjGEt-2LiWt0jzoReZ8Q0Sew/view?usp=sharing
# transactions -> https://drive.google.com/file/d/1Vdo7ev5UvPJ-NPGYyuLGBeGgLsQJ8bQg/view?usp=sharing

if __name__ == "__main__":
    greeter = Neo()
    greeter.print_greeting("hello, world")
    greeter.close()