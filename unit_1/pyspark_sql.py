'''
You've already tackled the basics, so let's continue your journey by building on that knowledge. 
Your first task is to complete a PySpark SQL query to select customers from the Netherlands.

Here's what you need to do:

Convert the DataFrame into a temporary view using the correct method.
Use the SQL query method to fetch customers from "Netherlands."
Take on this task to reinforce your grasp of PySpark SQL. You're doing well!
'''

from pyspark.sql import SparkSession

# Initialize a SparkSession
spark = SparkSession.builder.master("local").appName("SparkSQL").getOrCreate()

# Load the customer dataset from a CSV file into a DataFrame
df = spark.read.csv("customers.csv", header=True, inferSchema=True)

# TODO: Convert DataFrame into a temporary view to be used in SQL queries
df.createOrReplaceTempView("customers")

# TODO: Execute an SQL query to select customers based in a specific country, e.g., "Netherlands"
result_df = spark.sql("SELECT * FROM customers WHERE Country = 'Netherlands'")

# Display the result of the query
result_df.show()

# Stop the SparkSession to release resources
spark.stop()