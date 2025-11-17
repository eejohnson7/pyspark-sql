'''
As you've progressed through previous tasks, it's time to advance your PySpark SQL skills.

In this practice, you will complete a SQL query to retrieve customer data based on a certain criterion:

Convert the DataFrame into a temporary view so you can utilize SQL.
Execute a SQL query to fetch customers specifically from Germany.
Display the results to ensure the process is working as expected.
You've got this!
'''

from pyspark.sql import SparkSession

# Initialize a SparkSession
spark = SparkSession.builder.master("local").appName("SparkSQL").getOrCreate()

# Load the customer dataset from a CSV file into a DataFrame
df = spark.read.csv("customers.csv", header=True, inferSchema=True)

# TODO: Convert DataFrame into a temporary view to be used in SQL queries
df.createOrReplaceTempView("customers")

# TODO: Execute an SQL query to select customers based in a specific country, e.g., "Germany"
select = spark.sql("SELECT * FROM customers WHERE country = 'Germany'")

# TODO: Display the result of the query
select.show()

# Stop the SparkSession to release resources
spark.stop()