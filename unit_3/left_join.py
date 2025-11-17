'''
It's time to perform a different join using SQL and PySpark!

In this task, you'll change the JOIN operation to a LEFT JOIN in the provided SQL query.

Make the change and see how it affects the resulting dataset.
'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

# Initialize a SparkSession
spark = SparkSession.builder.master("local").appName("SQLJoins").getOrCreate()

# Load the primary customer dataset
df1 = spark.read.csv("customers.csv", header=True, inferSchema=True)

# Load a second dataset containing additional customer details, such as purchase history
df2 = spark.read.csv("customer_purchase_history.csv", header=True, inferSchema=True)

# Convert DataFrames into temporary views
df1.createOrReplaceTempView("customers")
df2.createOrReplaceTempView("purchase_history")

# TODO: Change the JOIN type to LEFT JOIN
join_query = """
SELECT c.*, p.PurchaseAmount
FROM customers c
LEFT JOIN purchase_history p
ON c.`Customer Id` = p.`Customer Id`
"""

# Execute query
joined_df = spark.sql(join_query)

# Display the joined DataFrame
joined_df.show()

# Stop the SparkSession to release resources
spark.stop()