# Creation of a NoSQL Database for Credit Card Fraud Detection

1. You have to use the scripts for the generation of at least 3 datasets (each one containing the three
tables described above) at an increased size (at least 50 Mbyte, 100 Mbyte, 200 Mbyte).
2. Define a conceptual model for the considered domain.
3. Choose one of the NOSQL systems considered in the course (Oracle + XML, MongoDB, Cassandra,
Neo4J) and provide a data modeling to optimize the execution of the following operations:
   - a) For each customer checks that the spending frequency and the spending amounts of the last
month is under the usual spending frequency and the spending amounts for the same
period.
    - b) For each terminal identify the possible fraudulent transactions. The fraudulent transactions
are those whose import is higher than 20% of the maximal import of the transactions
executed on the same terminal in the last month.
    - c) Given a user u, determine the “co-customer-relationships CC of degree k”. A user u’ is a co-
customer of u if you can determine a chain “u1-t1-u2-t2-...tk-1-uk“ such that u1=u, uk=u’, and for
each 1<=I,j<=k, ui <> uj, and t1 ,..tk-1 are the terminals on which a transaction has been
executed. Therefore, CCk(u)={u’| a chain exists between u and u’ of degree k}. Please, note
that depending on the adopted model, the computation of CC k(u) could be quite
complicated. Consider therefore at least the computation of CC3(u) (i.e. the co-costumer
relationships of degree 3).
    - d) Extend the logical model that you have stored in the NOSQL database by introducing the
following information (pay attention that this operation should be done once the NOSQL
database has been already loaded with the data extracted from the datasets):
        - i. Each transaction should be extended with (The values can be chosen randomly.):
           1. The period of the day {morning, afternoon, evening, night} in which the
transaction has been executed.
           2. The kind of products that have been bought through the transaction {high-
tech, food, clothing, consumable, other}
           3. The feeling of security expressed by the user. This is an integer value
between 1 and 5 expressed by the user when conclude the transaction.
        - ii. Customers that make more than three transactions from the same terminal
expressing a similar average feeling of security should be connected as
“buying_friends”. Therefore also this kind of relationship should be explicitly stored
in the NOSQL database and can be queried. Note, two average feelings of security
are considered similar when their difference is lower than 1.
    - e) For each period of the day identifies the number of transactions that occurred in that period,
and the average number of fraudulent transactions.
4. Create a script for loading the considered datasets in the chosen NOSQL and develop the scripts for
implementing the previous operations. Take in mind that depending on the identified system,
primitives of the query language should be embedded in a Python script.
datastore) in the generated datasets.
5. Evaluate the execution times for all the previous operations (also the one for updating the NOSQL