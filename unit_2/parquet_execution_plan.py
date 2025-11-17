'''
Now, let’s dive deeper into how different file formats impact query execution plans.

In this task, you need to change the data source loading from a CSV file to a Parquet file.

When observing the new execution plan, focus on these key differences:

Batched Reading: PySpark automatically processes data in batches with the Parquet format, using Batched: true, rather than 
processing data row by row. This results in more efficient operations. Format Specification: The plan shifts from CSV to 
Parquet, marking the change in data file type. These observations demonstrate how Parquet’s columnar storage can optimize 
performance in analytical queries compared to the row-based CSV format.
'''

from pyspark.sql import SparkSession

# Initialize a SparkSession
spark = SparkSession.builder.master("local").appName("UnderstandingSQLQueries").getOrCreate()

# TODO: Change this line to read from a Parquet file instead
df = spark.read.parquet("customers.parquet", header=True, inferSchema=True)

# Convert DataFrame into a temporary view for SQL querying
df.createOrReplaceTempView("customers")

# Define an SQL query to count the number of customers from each country
query = """
SELECT Country, COUNT(*) as CustomerCount
FROM customers
GROUP BY Country
"""

# Execute the query
result_df = spark.sql(query)

# Display the execution plan for the query
result_df.explain()

# Show the results of the query
result_df.show()

# Stop the SparkSession to release resources
spark.stop()