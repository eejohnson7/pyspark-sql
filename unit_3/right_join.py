'''
You've been doing well learning SQL joins in PySpark, so it's time to get hands-on.

In this task, your goal is to complete the code to execute a RIGHT JOIN:

Convert the provided DataFrames into temporary views. Name them "customers" and "purchase_history".
Define the SQL RIGHT JOIN query to combine data from both views based on the Customer Id. The query should select all 
columns from the "customers" view and the PurchaseAmount from the "purchase_history" view.
Execute the query to generate a new DataFrame.
Display the joined data, ensuring the results are as expected.
Dive in and make it all come together for smooth data analysis!
'''

from pyspark.sql import SparkSession

# Initialize a SparkSession
spark = SparkSession.builder.master("local").appName("SQLJoins").getOrCreate()

# Load the primary customer dataset
df1 = spark.read.csv("customers.csv", header=True, inferSchema=True)

# Load a second dataset containing additional customer details, such as purchase history
df2 = spark.read.csv("customer_purchase_history.csv", header=True, inferSchema=True)

# TODO: Convert df1 into a temporary view called "customers"
df1.createOrReplaceTempView("customers")

# TODO: Convert df2 into a temporary view called "purchase_history"
df2.createOrReplaceTempView("purchase_history")

# TODO:  Define a SQL RIGHT JOIN query to combine data based on the Customer Id
query = """
SELECT *
FROM customers c
RIGHT JOIN purchase_history p
ON c.`Customer Id` = p.`Customer Id`
"""

# TODO: Execute query
join_df = spark.sql(query)

# TODO: Display the joined DataFrame
join_df.show()

# Stop the SparkSession to release resources
spark.stop()