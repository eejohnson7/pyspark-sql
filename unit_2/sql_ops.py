'''
You've previously explored the setup of PySpark and SQL queries, establishing a solid foundation.

In this task, your objective is to focus on the SQL operations within the provided PySpark script:

Define an SQL query to calculate the average number of customers across all countries.
Utilize a subquery by enclosing it in parentheses to first count the number of customers for each country. 
Use this subquery as a derived table to then calculate the overall average.
Execute the defined SQL query using the temporary SQL view.
Display the execution plan for the SQL query to understand its behavior.
Show the results obtained from the query execution.
Feel free to reach out if you need any guidance or support while building the query; I'm here to help!
'''

from pyspark.sql import SparkSession

# Initialize a SparkSession
spark = SparkSession.builder.master("local").appName("UnderstandingSQLQueries").getOrCreate()

# Load the customer dataset from a CSV file into a DataFrame
df = spark.read.csv("customers.csv", header=True, inferSchema=True)

# Convert DataFrame into a temporary view for SQL querying
df.createOrReplaceTempView("customers")

# TODO: Define an SQL query to calculate the average number of customers across all countries
sql = """
SELECT AVG(customers) as overall_avg
FROM (SELECT Country, COUNT(*) as customers
FROM customers
GROUP BY Country)
"""

# TODO: Execute the query
result_df = spark.sql(sql)

# TODO: Display the execution plan for the query
result_df.explain()

# TODO: Show the results of the query
result_df.show()

# Stop the SparkSession to release resources
spark.stop()