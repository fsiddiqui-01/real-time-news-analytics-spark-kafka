
from pyspark.sql import SparkSession

# Start a local Spark session
spark = (
    SparkSession.builder
    .appName("CS4371-First-Spark-Test")
    .master("local[*]")
    .getOrCreate()
)

# Create a sample dataset
data = [
    ("Google", 10),
    ("Microsoft", 20),
    ("Apple", 15),
    ("OpenAI", 30)
]

# Convert the dataset into a Spark DataFrame
df = spark.createDataFrame(
    data,
    ["company", "mentions"]
)

# Display the data
df.show()

# Calculate total mentions
df.groupBy().sum("mentions").show()

# Shut down Spark
spark.stop()

