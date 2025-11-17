'''
Now, let's put your knowledge to the test by identifying and fixing an issue in the provided join operation.

Look through the provided code and spot the mistake, ensuring the query correctly combines the datasets.

Tackle this challenge to enhance your data manipulation skills!
'''

from pyspark.sql import SparkSession

# Initialize a SparkSession
spark = SparkSession.builder.master("local").appName("SQLJoins").getOrCreate()

# Load the primary customer dataset
df1 = spark.read.csv("customers.csv", header=True, inferSchema=True)

# Load a second dataset containing additional customer details, such as purchase history
df2 = spark.read.csv("customer_purchase_history.csv", header=True, inferSchema=True)

# Convert DataFrames into temporary views
df1.createOrReplaceTempView("customers")
df2.createOrReplaceTempView("purchase_history")

# Define an SQL JOIN query to combine data based on Customer Id
join_query = """
SELECT c.*, p.PurchaseAmount
FROM customers c
JOIN purchase_history p
ON c.`Customer Id` = p.`Customer Id`
"""

# Execute query
joined_df = spark.sql(join_query)

# Display the joined DataFrame
joined_df.show()

# Stop the SparkSession to release resources
spark.stop()