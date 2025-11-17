'''
In this task, you'll be exploring the average subscription period of customers in each country. By calculating the 
average number of days each customer has been subscribed, you can gain valuable insights into customer retention and 
loyalty by region.

Here’s what you’ll do:

Modify the SQL query to calculate the average subscription period for each country.
Utilize the AVG function to compute the average subscription period from the list of subscription days calculated.
Use the DATEDIFF function to calculate the difference in days between the CURRENT_DATE and each customer's subscription date. 
This will provide the period each customer has been subscribed.
Group the results by the Country column to observe how average subscription durations vary across countries.
'''

from pyspark.sql import SparkSession

# Initialize a SparkSession
spark = SparkSession.builder.master("local").appName("AverageSubscriptionPeriod").getOrCreate()

# Load the customer dataset from a CSV file into a DataFrame
df = spark.read.csv("customers.csv", header=True, inferSchema=True)

# Convert DataFrame into a temporary view for SQL querying
df.createOrReplaceTempView("customers")
df.show()

# TODO: Change the query to calculate the average subscription period for each country
query = """
SELECT Country, AVG(DATEDIFF(CURRENT_DATE, `Subscription Date`)) as AvgSubscriptionDate
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