# Operations description:

1. For each customer checks that the spending frequency and the spending amounts of the last month is under the usual spending frequency and the spending amounts for the same period.

   > spending frequency (last month) -> transaction count of the last month for this customer
   >
   > spending amounts (last month) -> transaction count of amount of the last month for this customer
   >
   > general -> transaction count of all customer for the last month (and for the amounts)

2. For each terminal identify the possible fraudulent transactions. The fraudulent transactions are those whose import is higher than 20% of the maximal import of the transactions executed on the same terminal in the last month.

   > Fraudolent -> transaction with import > 20% of the maximal import of the transactions on the same terminal in the last month

3. Given a user $u$, determine the “co-customer-relationships $CC$ of degree $k$”. 
   A user $u’$ is a co-customer of $u$ if you can determine a chain $u_1-t_1-u_2-t_2-...t_{k-1}-u_k$ such that $u1=u, u_k=u’$, and for each $1<=I,j<=k, u_i <> u_j$, and $t_1 ,..t_{k-1}$ are the terminals on which a transaction has been executed. 
   Therefore, $CC_k(u)={u’|\space a\space chain\space exists\space between\space u\space and\space u’\space of\space degree\space k}.$ 
   Please, note that depending on the adopted model, the computation of $CC_k(u)$ could be quite complicated. |
   Consider therefore at least the computation of $CC_3(u)$ (i.e. the co-costumer relationships of degree 3).

4. Extend the logical model that you have stored in the NOSQL database by introducing the following information (pay attention that this operation should be done once the NOSQL database has been already loaded with the data extracted from the datasets):

   1. Each transaction should be extended with:

      > The values can be chosen randomly.

      1. The **period of the day** {morning, afternoon, evening, night} in which the transaction has been executed.
      2. The **kind of products** that have been bought through the transaction {high-tech, food, clothing, consumable, other}
      3. The **feeling of security** expressed by the user. This is an integer value 5 between 1 and 5 expressed by the user when conclude the transaction. 

   2. **Customers** that make **more than three transactions from the same terminal** **expressing a similar average feeling of security** **should be connected as “buying_friends”**. 

      Therefore also this kind of relationship should be explicitly stored in the NOSQL database and can be queried. Note, two average feelings of security are considered similar when their difference is lower than 1.

5. For each period of the day identifies the number of transactions that occurred in that period, and the average number of fraudulent transactions.