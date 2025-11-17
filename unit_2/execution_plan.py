'''
You've made excellent progress in working with SQL queries and execution plans.

In this exercise, you will fill in the correct method to display the execution plan before running the query.

Dive into these steps to reinforce your understanding and showcase your skills!
'''

from pyspark.sql import SparkSession

# Initialize a SparkSession
spark = SparkSession.builder.master("local").appName("UnderstandingSQLQueries").getOrCreate()

# Load the customer dataset from a CSV file into a DataFrame
df = spark.read.csv("customers.csv", header=True, inferSchema=True)

# Convert DataFrame into a temporary view for SQL querying
df.createOrReplaceTempView("customers")

# Define an SQL query to count the number of customers from each country
query = """
SELECT Country, COUNT(*) as CustomerCount
FROM customers
GROUP BY Country
"""

# Execute the query
result_df = spark.sql(query)

# TODO: Display the execution plan for the query
result_df.explain()

# Show the results of the query
result_df.show()

# Stop the SparkSession to release resources
spark.stop()