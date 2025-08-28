from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, date_format

spark = SparkSession.builder.appName("SilverTransform").getOrCreate()

orders = spark.read.parquet("data/bronze/orders/")

# Convert to proper types
orders = orders.withColumn("order_date", to_date("order_date")) \
               .withColumn("delivery_date", to_date("delivery_date")) \
               .withColumn("order_yyyymm", date_format("order_date", "yyyy-MM"))

orders.write.mode("overwrite").partitionBy("order_yyyymm").parquet("data/silver/orders/")

spark.stop()
