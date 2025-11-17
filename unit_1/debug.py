'''
In this next task, you'll address issues in a script meant to execute an SQL query using PySpark.

Ensure the query retrieves data accurately from the temporary view.

Keep it up!
'''

from pyspark.sql import SparkSession

# Initialize a SparkSession
spark = SparkSession.builder.master("local").appName("SparkSQL").getOrCreate()

# Load the customer dataset from a CSV file into a DataFrame
df = spark.read.csv("customers.csv", header=True, inferSchema=True)

# Convert DataFrame into a temporary view to be used in SQL queries
df.createOrReplaceTempView("customers")

# Execute an SQL query to select customers based in a specific country, e.g., "Brazil"
# result_df = df.sql("SELECT * FROM customers WHERE Country = 'Brazil'")
result_df = spark.sql("SELECT * FROM customers WHERE Country = 'Brazil'")

# Display the result of the query
result_df.show()

# Stop the SparkSession to release resources
spark.stop()