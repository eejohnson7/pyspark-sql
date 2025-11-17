'''
In this task, you'll expand your understanding of PySpark's User-Defined Functions (UDFs) by transforming an existing UDF 
for a different application. Here are the step-by-step instructions:

Modify the Function:

Create a new function named email_length that calculates the length of a customer's email using the len() method.
Convert the Python Function to a PySpark UDF:

Convert the email_length function into a PySpark UDF using udf and set the return type to IntegerType.
Register the UDF with Spark:

Register this newly converted PySpark UDF with Spark using the name "email_length_udf".
Adjust the SQL Query:

Modify the SQL query to utilize the email_length_udf, creating a new column named EmailLength that contains the calculated 
lengths of customer emails.
Follow each step to accomplish this transformation seamlessly!
'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

# Initialize a SparkSession
spark = SparkSession.builder.master("local").appName("UDFunction").getOrCreate()

# Load the customer dataset from a CSV file into a DataFrame
df = spark.read.csv("customers.csv", header=True, inferSchema=True)

# Create a temporary view of the DataFrame
df.createOrReplaceTempView("customers")

# TODO: Change the UDF to count the length of the customer's email using the len() method
#  - Name the function as email_length
def email_length(email):
    return len(email)

# TODO: Convert the 'email_length' function to a PySpark UDF using IntegerType
# - Name the udf as email_length_udf
email_length_udf = udf(email_length, IntegerType())

# TODO: Register the email_length_udf UDF with Spark as "email_length_udf"
spark.udf.register("email_length_udf", email_length_udf)

# TODO: Modify the query to calculate email lengths using email_length_udf'
query = """
SELECT email_length_udf(`Email`) AS EmailLength
FROM customers
"""

# Execute the query
result_df = spark.sql(query)

# Display the result
result_df.show()

# Stop the SparkSession to release resources
spark.stop()