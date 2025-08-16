from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# --------------------------
# 1. Spark Session with PostgreSQL JDBC Driver
# --------------------------
spark = SparkSession.builder \
    .appName("Ecommerce_ETL") \
    .config("spark.jars", "file:///C:/Users/asus/Downloads/postgresql-42.2.27.jar") \
    .getOrCreate()

# --------------------------
# 2. Load CSVs
# --------------------------
customers_df = spark.read.csv("data/customers.csv", header=True, inferSchema=True)
products_df = spark.read.csv("data/products.csv", header=True, inferSchema=True)
orders_df = spark.read.csv("data/orders.csv", header=True, inferSchema=True)

# --------------------------
# 3. Data Cleaning
# --------------------------
# Filter only completed orders
orders_df = orders_df.filter(col("status") == "Completed")

# --------------------------
# 4. Join DataFrames
# --------------------------
fact_df = orders_df \
    .join(customers_df, "customer_id") \
    .join(products_df, "product_id")

# --------------------------
# 5. Add Revenue Column
# --------------------------
fact_df = fact_df.withColumn("revenue", col("quantity") * col("price_per_unit"))

# --------------------------
# 6. Save to PostgreSQL
# --------------------------
fact_df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/ecommerce") \
    .option("dbtable", "fact_orders") \
    .option("user", "postgres") \
    .option("password", "user") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

print("✅ Data successfully loaded into PostgreSQL table 'fact_orders'")
