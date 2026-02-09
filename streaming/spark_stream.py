from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType

# Create Spark Session
spark = SparkSession.builder \
    .appName("RealTimeMLPipeline") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Define schema
schema = StructType() \
    .add("transaction_id", StringType()) \
    .add("amount", DoubleType()) \
    .add("user_id", StringType())

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions") \
    .load()

# Convert value from bytes to string
df = df.selectExpr("CAST(value AS STRING)")

# Parse JSON
df = df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Dummy ML logic (Fraud Detection Rule)
df = df.withColumn(
    "prediction",
    (col("amount") > 1000).cast("int")
)

# Output to console
query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
