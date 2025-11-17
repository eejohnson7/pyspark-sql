'''
Next, let's build on what you've learned by making small but significant changes.

Your task is to:

Change the temporary view name from "customers" to "clients".
Modify the SQL query to filter customers from "Australia" instead of "Brazil".
This exercise will help reinforce your understanding of temporary views and SQL queries.
'''

from pyspark.sql import SparkSession

# Initialize a SparkSession
spark = SparkSession.builder.master("local").appName("SparkSQL").getOrCreate()

# Load the customer dataset from a CSV file into a DataFrame
df = spark.read.csv("customers.csv", header=True, inferSchema=True)

# TODO: Change the temporary view name to "clients"
df.createOrReplaceTempView("clients")

# TODO: Change the SQL query to select clients based in "Australia"
result_df = spark.sql("SELECT * FROM clients WHERE Country = 'Australia'")

# Display the result of the query
result_df.show()

# Stop the SparkSession to release resources
spark.stop()