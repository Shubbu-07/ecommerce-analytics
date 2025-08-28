from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, date_format

spark = SparkSession.builder.appName("BronzeIngest").getOrCreate()

# Example: ingest orders
df = spark.read.csv("../data/raw/orders.csv", header=True, inferSchema=True)

df = df.withColumn("ingested_at", current_timestamp()) \
       .withColumn("ingestion_date", date_format("ingested_at", "yyyy-MM-dd"))

df.write.mode("overwrite").parquet("../data/bronze/orders/")

spark.stop()
