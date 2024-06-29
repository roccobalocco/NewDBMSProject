from neo4j import GraphDatabase
import os
from datetime import date, datetime
from objects import Customer, FileType, Transaction

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

    def free_query(self, query):
        with self.driver.session() as session:
            return session.execute_write(self._free_query, query)
        
    def free_query_single(self, query):
        with self.driver.session() as session:
            result = session.execute_write(self._free_query_single, query)
            return result

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
        result = tx.run(query)
        return result.data()
    
    @staticmethod
    def _free_query_single(tx, query):
        result = tx.run(query)
        return result.single()
    
    # Start of operations (a) functions
    def get_customer_under_average(self, dt_start: datetime, dt_end: datetime)-> list[Customer]:
        """ Get customer under average for spending amounts and frequency of spending between dt_start and dt_end, 
            by comparing them to the average of this period
            (considering period as the same day&month over all the years registered in the database)
        
                Args:
                dt_start(datetime): date that states the start of the period to take in account 
                dt_end(datetime): date that states the end of the period to take in account
    
            Returns:
                A list representing all the customers that have their amounts and frequency of spending in the period in this year 
                less than the average of this period all over the years
        """
        avg_spending_amount = self.get_period_average_spending_amounts(dt_start, dt_end)
        avg_spending_frequency = self.get_period_average_spending_frequency(dt_start, dt_end)
        print('avg_spending_amount:', avg_spending_amount, 'avg_spending_frequency:', avg_spending_frequency)
        query = f"""
        MATCH (c:Customer)  -[t:Transaction]-> (:Terminal)
        WHERE datetime({{epochMillis: apoc.date.parse(t.TX_DATETIME, 'ms', 'yyyy-MM-dd HH:mm:ss')}}) >= '{dt_start}' 
        AND 
        datetime({{epochMillis: apoc.date.parse(t.TX_DATETIME, 'ms', 'yyyy-MM-dd HH:mm:ss')}}) <= '{dt_end}'
        WITH c, AVG(t.TX_AMOUNT) as avg_amount, COUNT(t) as nb_tx
        WHERE avg_amount < {avg_spending_amount} AND nb_tx < {avg_spending_frequency}
        RETURN collect(c) as customers
        """
        print("get_customer_under_average query:\n", query)

        customers = self.free_query(query)
        print(customers)
        return customers  # type: ignore
    def get_period_average_spending_amounts(self, dt_start:date, dt_end:date)-> float:
        """ Get the average of spending amounts in a given period between dt_start and dt_end for all the years in the database
            
            Args:
                dt_start(datetime): date that states the start of the period to take in account (not consider the year)
                dt_end(datetime): date that states the end of the period to take in account (not consider the year) 
    
            Returns:
                A float stating the spending average in the period
        """
        query = f"""
        MATCH ()-[t:Transaction]->(:Terminal)
        WHERE 
            datetime({{epochMillis: apoc.date.parse(t.TX_DATETIME, 'ms', 'yyyy-MM-dd HH:mm:ss')}}).month >= {dt_start.month} 
            AND 
            datetime({{epochMillis: apoc.date.parse(t.TX_DATETIME, 'ms', 'yyyy-MM-dd HH:mm:ss')}}).day >= {dt_start.day}
            AND 
            datetime({{epochMillis: apoc.date.parse(t.TX_DATETIME, 'ms', 'yyyy-MM-dd HH:mm:ss')}}).month <= {dt_end.month} 
            AND
            datetime({{epochMillis: apoc.date.parse(t.TX_DATETIME, 'ms', 'yyyy-MM-dd HH:mm:ss')}}).day <= {dt_end.day}
        RETURN AVG(t.TX_AMOUNT) as avg_spending_amount
        """

        avg_spending_amount = self.free_query_single(query)
        if (avg_spending_amount is None):
            return 0.
        return float(avg_spending_amount["avg_spending_amount"])  # type: ignore
    def get_period_average_spending_frequency(self, dt_start:date, dt_end:date)-> float:
        """ Get the average of spending frequency in a given period between dt_start and dt_end for all the years in the database
            Args:
                dt_start(datetime): date that states the start of the period to take in account (not consider the year)
                dt_end(datetime): date that states the end of the period to take in account (not consider the year) 
    
            Returns:
                A float stating the spending frequency in the period
        """
        query = f"""
        MATCH (c:Customer)-[t:Transaction]->(:Terminal)
        WHERE
        datetime({{epochMillis: apoc.date.parse(t.TX_DATETIME, 'ms', 'yyyy-MM-dd HH:mm:ss')}}).month >= {dt_start.month}
        AND 
        datetime({{epochMillis: apoc.date.parse(t.TX_DATETIME, 'ms', 'yyyy-MM-dd HH:mm:ss')}}).day >= {dt_start.day}
        AND 
        datetime({{epochMillis: apoc.date.parse(t.TX_DATETIME, 'ms', 'yyyy-MM-dd HH:mm:ss')}}).month <= {dt_end.month}
        AND 
        datetime({{epochMillis: apoc.date.parse(t.TX_DATETIME, 'ms', 'yyyy-MM-dd HH:mm:ss')}}).day <= {dt_end.day}
        WITH COUNT(t) as transaction_number, COUNT(DISTINCT c) as customer_number
        RETURN transaction_number*1.0/customer_number as avg_spending_frequency
        """
        avg_spending_frequency  = self.free_query_single(query)

        if (avg_spending_frequency is None):
            return 0.
        return float(avg_spending_frequency["avg_spending_frequency"])  # type: ignore
    # End of operations (a) functions
    
    # Start of operations (b) functions
    def get_fraudolent_transactions(self, terminal_id:int, dt_start:date, dt_end:date)-> list:
        """ Get all the fraudolent transactions that have an import higher than 20% 
            of the maximal import of the transactions executed on the same terminal in the last month
            
            Returns:
                A list of transactions that are considered fraudolent
        """
        maximal_import = self.get_terminal_max_import_last_month(terminal_id, dt_start, dt_end)
        maximal_import_20 = maximal_import + (maximal_import * 0.2)
        query = f"""
        MATCH (:Terminal {{TERMINAL_ID: {terminal_id}}}) -[t:Transaction]- ()
        WHERE t.TX_AMOUNT >= {maximal_import_20}
        RETURN collect(t) as fraudolent_transactions
        """
        
        fraudolent_transactions = self.free_query(query)
        return fraudolent_transactions  # type: ignore
    def get_terminal_max_import_last_month(self, terminal_id:int, dt_start:date, dt_end:date)-> float:
        """ Get the maximal import of the transactions executed on the terminal terminal_id in the last month
            
            Args:
                terminal_id(int): the id of the terminal to consider
                dt_start(datetime): date that states the start of the period to take in account
                dt_end(datetime): date that states the end of the period to take in account 
    
            Returns:
                A float representing the maximal import of the transactions executed on the terminal terminal_id in the last month
        """
        query = f"""
        MATCH (t:Terminal {{TERMINAL_ID: {terminal_id}}}) -[tr:Transaction]-> (:Customer)
        WHERE t.TX_DATETIME >= dt_start AND t.TX_DATETIME <= dt_end
        RETURN MAX(tr.TX_AMOUNT) as max_import
        """
        
        maximal_import = self.free_query(query)
        if (maximal_import is None):
            return 0.
        return float(maximal_import) # type: ignore
    # End of operations (b) functions
    
    # Start of operations (c) functions
    def get_co_customer_relationships_of_degree_k(self, u:int, k:int)-> list[Customer]:
        """ Get the co-customer-relationships of degree k for the user u
            
            Args:
                u(int): the id of the user to consider
                k(int): the degree of the co-customer-relationships
    
            Returns:
                A list of the users that have a co-customer-relationship of degree k with the user u
        """
        return []
    def set_co_customer_relationships(self, u:int, l:list)-> None:
        """ Set the co-customer-relationships for the user u
            
            Args:
                u(int): the id of the user to consider
                l(list(int)): the list of the users that have a co-customer-relationship with the user u
        """
    # End of operations (c) functions
    
    # Start of operations (d) functions
    def extend_neo(self)-> None:
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
        self.extend_neo_with_period()
        self.extend_neo_with_kind_of_product()
        self.extend_neo_with_feeling_of_security()
        self.connect_buying_friends()

    def extend_neo_with_period(self)-> None:
        """ Extend the logical model that you have stored in the NOSQL database by introducing the following information:
            Each transaction should be extended with:
                - The period of the day {morning, afternoon, evening, night} in which the transaction has been executed.
        """
        query = """
        MATCH (t:Transaction)
        SET t.PERIOD_OF_DAY = CASE
            WHEN hour(t.TX_DATETIME) >= 6 AND hour(t.TX_DATETIME) < 12 THEN 'morning'
            WHEN hour(t.TX_DATETIME) >= 12 AND hour(t.TX_DATETIME) < 18 THEN 'afternoon'
            WHEN hour(t.TX_DATETIME) >= 18 AND hour(t.TX_DATETIME) < 24 THEN 'evening'
            ELSE 'night'
        END
        """
        
        self.free_query(query)

    def extend_neo_with_kind_of_product(self)-> None:
        """ Extend the logical model that you have stored in the NOSQL database by introducing the following information:
            Each transaction should be extended with:
                - The kind of products that have been bought through the transaction {high-tech, food, clothing, consumable, other}
        """
        query = """
        MATCH (t:Transaction)
        SET t.KIND_OF_PRODUCT = CASE
            WHEN t.PRODUCT_TYPE = 'high-tech' THEN 'high-tech'
            WHEN t.PRODUCT_TYPE = 'food' THEN 'food'
            WHEN t.PRODUCT_TYPE = 'clothing' THEN 'clothing'
            WHEN t.PRODUCT_TYPE = 'consumable' THEN 'consumable'
            ELSE 'other'
        END
        """
        
        self.free_query(query)
        
    def extend_neo_with_feeling_of_security(self)-> None:
        """ Extend the logical model that you have stored in the NOSQL database by introducing the following information:
            Each transaction should be extended with:
                - The feeling of security expressed by the user. This is an integer value between 1 and 5 expressed by the user when conclude the transaction.
                    The values can be chosen randomly.
        """
        query = """
        MATCH (t:Transaction)
        SET t.FEELING_OF_SECURITY = toInteger(rand() * 5) + 1
        """
        
        self.free_query(query)

    def connect_buying_friends(self)-> None:
        """ Connect customers that make more than three transactions from the same terminal expressing a similar average feeling of security as "buying_friends".
        """
        query = """
        MATCH (t:Transaction)-[:EXECUTED_ON]->(terminal:Terminal)<-[:EXECUTED_ON]-(other:Transaction)
        WHERE t.CUSTOMER_ID = other.CUSTOMER_ID AND t.TERMINAL_ID = other.TERMINAL_ID
        WITH t.CUSTOMER_ID AS customer_id, terminal.TERMINAL_ID AS terminal_id, AVG(t.FEELING_OF_SECURITY) AS avg_feeling_of_security
        GROUP BY customer_id, terminal_id
        HAVING COUNT(t) > 3 AND abs(AVG(t.FEELING_OF_SECURITY) - avg_feeling_of_security) < 1
        MATCH (c1:Customer {CUSTOMER_ID: customer_id})-[:EXECUTED]->(t1:Transaction)-[:EXECUTED_ON]->(terminal1:Terminal {TERMINAL_ID: terminal_id})
        MATCH (c2:Customer)-[:EXECUTED]->(t2:Transaction)-[:EXECUTED_ON]->(terminal2:Terminal {TERMINAL_ID: terminal_id})
        WHERE c1 <> c2
        CREATE (c1)-[:BUYING_FRIEND]->(c2)
        """
        
        self.free_query(query)
    # End of operations (d) functions
    
    # Start of operations (e) functions
    def get_transactions_per_period(self, dt_start: date, dt_end: date)-> dict[str, list[Transaction]]:
        """ Get the transactions that occurred in each period of the day
            
            Args:
                dt_start(datetime): date that states the start of the period to take in account (not consider the year)
                dt_end(datetime): date that states the end of the period to take in account (not consider the year) 
            
            Returns:
                A dict representing the transactions that occurred in each period of the day
        """
        query = f"""
        MATCH (t:Transaction)
        WHERE datetime({{epochMillis: apoc.date.parse(t.TX_DATETIME, 'ms', 'yyyy-MM-dd HH:mm:ss')}}) >= date('{dt_start}') AND datetime({{epochMillis: apoc.date.parse(t.TX_DATETIME, 'ms', 'yyyy-MM-dd HH:mm:ss')}}) <= date('{dt_end}')
        RETURN t.PERIOD_OF_DAY, collect(t) as transactions_per_period
        """
        
        transactions_per_period = self.free_query(query)
        return transactions_per_period
    def get_fraudolent_transactions_per_period(self, dt_start: date, dt_end: date)-> dict[str, list[Transaction]]:
        """ Get the fraudulent transactions that occurred in each period of the day and the average number of transactions
            
            Args:
                dt_start(datetime): date that states the start of the period to take in account (not consider the year)
                dt_end(datetime): date that states the end of the period to take in account (not consider the year) 
            
            Returns:
                A dict representing the fraudulent transactions that occurred in each period of the day and the average number of transactions
        """
        query = f"""
        MATCH (t:Transaction)
        WHERE datetime({{epochMillis: apoc.date.parse(t.TX_DATETIME, 'ms', 'yyyy-MM-dd HH:mm:ss')}}) >= '{dt_start}' AND date(t.TX_DATETIME) <= '{dt_end}'
        AND t.IS_FRAUD = true
        RETURN t.PERIOD_OF_DAY, collect(t) as fraudulent_transactions, avg(count(t)) as average_number_of_transactions
        """
        
        result = self.free_query(query)
        return result

if __name__ == "__main__":
    greeter = Neo()
    # This query show nothing as a result, but don't worry and have a look at the db
    # There are a lot of costumer with small transactions number but large spending amount and viceversa, so....
    # greeter.get_customer_under_average(datetime(2019, 1, 1), datetime(2019, 2, 1))
    greeter.close()