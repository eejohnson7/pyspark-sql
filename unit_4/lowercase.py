'''
Nice work on grasping UDFs in PySpark! Now, let's put that knowledge to use by completing some missing parts in the code.

In this exercise, you'll need to:

Define a function to change names to lowercase.
Convert this function to a PySpark UDF.
Register and use the UDF in a SQL query.
Execute the query and display the results.
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

# TODO: Define a Python function named `lowercase_name`
def lowercase_name(name):
    # TODO: Return a lowercased string using the lower() method
    return name.lower()

# TODO: Convert the Python function to a PySpark UDF and set StringType()
lowercase_name_udf = udf(lowercase_name, StringType())

# TODO: Register the UDF with Spark
spark.udf.register("lowercase_name_udf", lowercase_name_udf)

# TODO: Write a SQL query using the UDF to select lowercase first names and original last names
query = """
SELECT lowercase_name_udf(`First Name`), `Last Name`
FROM customers
"""

# TODO: Execute the query to transform customer names
transformed_df = spark.sql(query)

# TODO: Display the resulting DataFrame
transformed_df.show()

# Stop the SparkSession to release resources
spark.stop()