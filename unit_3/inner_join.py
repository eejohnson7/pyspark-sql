'''
You've observed SQL joins in PySpark, and now it's time to practice integrating datasets using these concepts.

For this task, you'll need to fill in the missing parts of a SQL join query by:

Writing the SQL query to combine data using an INNER JOIN.
Ensuring the correct views are joined on the Customer Id.
Give it a shot and see how your data integration skills have improved!
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

# TODO: Complete the SQL JOIN query to combine data based on Customer Id
join_query = """
SELECT c.*, p.PurchaseAmount
FROM customers c
INNER JOIN purchase_history p
ON c.`Customer Id` = p.`Customer Id`
"""

# Execute query
joined_df = spark.sql(join_query)

# Display the joined DataFrame
joined_df.show()

# Stop the SparkSession to release resources
spark.stop()