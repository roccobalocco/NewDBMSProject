from neo4j import GraphDatabase
import os
from enum import Enum
from datetime import datetime

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


    # Start of operations (a) functions
    def get_customer_under_average(self, dt_start: datetime, dt_end: datetime)-> list(any):
        """ Get customer under average for spending amounts and frequency of spending between dt_start and dt_end, by comparing them to the average of this period
        (considering period as the same day&month over all the years registered in the database)
            Args:
                dt_start(datetime): date that states the start of the period to take in account 
                dt_end(datetime): date that states the end of the period to take in account
    
            Returns:
                A list representing all the customers that have their amounts and frequency of spending in the period less than the average of this period
        """
        return ''
    def get_period_average_spending_amounts(self, dt_start:Date, dt_end:Date)-> float:
        """ Get the average of spending amounts in a given period between dt_start and dt_end for all the years in the database
            
            Args:
                dt_start(datetime): date that states the start of the period to take in account (not consider the year)
                dt_end(datetime): date that states the end of the period to take in account (not consider the year) 
    
            Returns:
                A float stating the spending average in the period
        """
        return ''
    def get_period_average_spending_frequency(self, dt_start:Date, dt_end:Date)-> float:
        """ Get the average of spending frequency in a given period between dt_start and dt_end for all the years in the database
            Args:
                dt_start(datetime): date that states the start of the period to take in account (not consider the year)
                dt_end(datetime): date that states the end of the period to take in account (not consider the year) 
    
            Returns:
                A float stating the spending frequency in the period
        """
        return ''
    # End of operations (a) functions
    
    # Start of operations (b) functions
    def get_fraudolent_transactions(self, )-> list(any):
        """ Get all the fraudolent transactions that have an import higher than 20% 
            of the maximal import of the transactions executed on the same terminal in the last month
            
            Returns:
                A list of transactions that are considered fraudolent
        """
        return []
    def get_terminal_max_import_last_month(self, terminal_id:int)-> float:
        """ Get the maximal import of the transactions executed on the terminal terminal_id in the last month
            
            Args:
                terminal_id(int): the id of the terminal to consider
    
            Returns:
                A float representing the maximal import of the transactions executed on the terminal terminal_id in the last month
        """
        return 0.1
    # End of operations (b) functions
    
    # Start of operations (c) functions
    def get_co_customer_relationships_of_degree_k(self, u:int, k:int)-> list(int):
        """ Get the co-customer-relationships of degree k for the user u
            
            Args:
                u(int): the id of the user to consider
                k(int): the degree of the co-customer-relationships
    
            Returns:
                A list of the users that have a co-customer-relationship of degree k with the user u
        """
        return []
    def set_co_customer_relationships(self, u:int, l:list(int))-> None:
        """ Set the co-customer-relationships for the user u
            
            Args:
                u(int): the id of the user to consider
                l(list(int)): the list of the users that have a co-customer-relationship with the user u
        """
    # End of operations (c) functions
    
    # Start of operations (d) functions
    def extend_neo(self, )-> None:
        """ Extend the logical model that you have stored in the NOSQL database by introducing the following information:
            Each transaction should be extended with:
                - The period of the day {morning, afternoon, evening, night} in which the transaction has been executed.
                - The kind of products that have been bought through the transaction {high-tech, food, clothing, consumable, other}
                - The feeling of security expressed by the user. This is an integer value between 1 and 5 expressed by the user when conclude the transaction.
                    The values can be chosen randomly.
            Customers that make more than three transactions from the same terminal expressing a similar average feeling of security should be connected as “buying_friends”.
            Therefore also this kind of relationship should be explicitly stored in the NOSQL database and can be queried. 
            Note, two average feelings of security are considered similar when their difference is lower than 1.
        """
    def extend_neo_with_period(self, )-> None:
        """ Extend the logical model that you have stored in the NOSQL database by introducing the following information:
            Each transaction should be extended with:
                - The period of the day {morning, afternoon, evening, night} in which the transaction has been executed.
        """
    def extend_neo_with_kind_of_product(self, )-> None:
        """ Extend the logical model that you have stored in the NOSQL database by introducing the following information:
            Each transaction should be extended with:
                - The period of the day {morning, afternoon, evening, night} in which the transaction has been executed.
        """
    def extend_neo_with_feeling_of_security(self, )-> None:
        """ Extend the logical model that you have stored in the NOSQL database by introducing the following information:
            Each transaction should be extended with:
                - The period of the day {morning, afternoon, evening, night} in which the transaction has been executed.
        """
    def get_buying_friends(self, u:int)-> list(int):
        """ Get the buying friends of the user u
            
            Args:
                u(int): the id of the user to consider
    
            Returns:
                A list of the users that are buying friends of the user u
        """
        return []
    def set_buying_friends(self, u:int, l:list(int))-> None:
        """ Set the buying friends for the user u
            
            Args:
                u(int): the id of the user to consider
                l(list(int)): the list of the users that are buying friends of the user u
        """
        return []
    # End of operations (d) functions
    
    # Start of operations (e) functions
    def get_transactions_per_period(self, dt_start: datetime, dt_end: datetime)-> dict():
        """ Get the number of transactions that occurred in each period of the day
            
            Args:
                dt_start(datetime): date that states the start of the period to take in account (not consider the year)
                dt_end(datetime): date that states the end of the period to take in account (not consider the year) 
            
            Returns:
                An dicr representing the number of transactions that occurred in each period of the day
        """
        return {}
    def get_fraudolent_transactions_per_period(self, dt_start: datetime, dt_end: datetime)-> dict()
        """ Get the number of fraudulent transactions that occurred in each period of the day and the average number of transactions
            
            Args:
                dt_start(datetime): date that states the start of the period to take in account (not consider the year)
                dt_end(datetime): date that states the end of the period to take in account (not consider the year) 
            
            Returns:
                An dict representing the number of fraudulent transactions that occurred in each period of the day and the average number of transactions
        """
        return {}

if __name__ == "__main__":
    greeter = Neo()
    greeter.close()