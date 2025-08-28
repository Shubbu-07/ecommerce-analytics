from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, col, countDistinct

spark = SparkSession.builder.appName("GoldAnalytics").getOrCreate()

orders = spark.read.parquet("../data/silver/orders/")
items = spark.read.csv("../data/raw/order_items.csv", header=True, inferSchema=True)
payments = spark.read.csv("../data/raw/order_payments.csv", header=True, inferSchema=True)

# Join & build fact table
fact_sales = (orders.alias("o")
    .join(items.alias("i"), "order_id", "left")
    .join(payments.alias("p"), "order_id", "left")
    .groupBy("o.order_id", "o.customer_id", "o.order_date")
    .agg(
        _sum(col("i.quantity") * col("i.price")).alias("gross_amount"),
        _sum("i.freight").alias("freight"),
        _sum("p.payment_value").alias("paid_amount")
    )
)

fact_sales.write.mode("overwrite").parquet("data/gold/fact_sales/")

# Example KPI: Total revenue & AOV
fact_sales.createOrReplaceTempView("fact_sales")
result = spark.sql("""
SELECT 
    SUM(paid_amount) AS total_revenue,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(paid_amount)/COUNT(DISTINCT order_id) AS aov
FROM fact_sales
""")
result.show()

spark.stop()
