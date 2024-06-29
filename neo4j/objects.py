from enum import Enum


class FileType(Enum):
    CUSTOMERS = 0
    TERMINALS = 1
    TRANSACTIONS = 2

class Customer:
    def __init__(self, line):
        self.CUSTOMER_ID = line.CUSTOMER_ID
        self.x_customer_id = line.x_customer_id
        self.y_customer_id = line.y_customer_id
        self.mean_amount = float(line.mean_amount)
        self.std_amount = float(line.std_amount)
        self.mean_nb_tx_per_day = float(line.mean_nb_tx_per_day)
        self.available_terminals = line.available_terminals
        self.nb_terminals = int(line.nb_terminals)

class Terminal:
    class Terminal:
        def __init__(self, line):
            self.TERMINAL_ID = line.TERMINAL_ID
            self.x_terminal_id = line.x_terminal_id
            self.y_terminal_id = line.y_terminal_id

class Transaction:
    def __init__(self, line):
        self.TRANSACTION_ID = line[1]
        self.TX_DATETIME = line[2]
        self.TX_AMOUNT = float(line[5])
        self.TX_TIME_SECONDS = int(line[6])
        self.TX_TIME_DAYS = int(line[7])
        self.TX_FRAUD = line[8]
        self.TX_FRAUD_SCENARIO = line[9]