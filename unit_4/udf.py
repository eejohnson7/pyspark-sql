'''
You've done an excellent job learning about utilizing UDFs in PySpark. Let's see how well you can apply this 
knowledge by filling in some missing pieces in the code.

In this task, you'll:

Convert a Python function to a Spark UDF.
Register the UDF to make it usable in SQL queries.
Use the UDF in a SQL query to transform customer names to uppercase.
Give it your best and continue building those skills!
'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Initialize a SparkSession
spark = SparkSession.builder.master("local").appName("UDFunction").getOrCreate()

# Load the customer dataset from a CSV file into a DataFrame
df = spark.read.csv("customers.csv", header=True, inferSchema=True)

# Create a temporary view of the DataFrame
df.createOrReplaceTempView("customers")

# Define a UDF that formats the customer's name in uppercase
def format_name(name):
    return name.upper()

# TODO: Convert the Python function to a PySpark UDF to make it available in SQL queries
format_name_udf = udf(format_name, StringType())

# TODO: Register the UDF with Spark
spark.udf.register("format_name_udf", format_name_udf)

# TODO: Use the registered UDF in the SQL query to format the 'First Name' column
query = """
SELECT format_name_udf(`First Name`) AS FormattedName, `Last Name`
FROM customers
"""

# Use the query to format customer names
result_df = spark.sql(query)

# Display the formatted result
result_df.show()

# Stop the SparkSession to release resources
spark.stop()