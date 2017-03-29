# DataOne_StoreFinder
A StoreFinder App to process big data and give solution.

That this is a weighted set cover problem which is NP-complete.
A greedy approach which has an approximate complexity of log N(Bounded By)

The basic logic as follows:
1.Read the data as text File and split it by the delimiter "," to make a RDD.
2.Read the CLI arguments to make a Set of it.(Products to be bought.)
3.Find all the Shops which have all the Products required by us and save it to an array.
4.Filter to those :
a. Those shops which have all the products.
b.Filter again to those which contain at least one relevant product.
5.Call LocalMinimum() for each ShopID(multiple rows of same Shop)
6.Call GlobalMinimum() after finding all the local minimum.

ToDO
1.Build a wrapper .sh to use spark-submit to get input in specified format.

2.Use Maven/sbt to Build the Project
    Env: Java 8
         Scala 2.11
         Spark 2.0.2

