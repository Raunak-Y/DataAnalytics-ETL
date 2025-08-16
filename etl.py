from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, year, month

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("PySpark ETL to PostgreSQL") \
    .config("spark.jars", "file:///C:/Users/asus/Downloads/postgresql-42.2.27.jar") \
    .getOrCreate()

# =======================
# Step 1: Load CSV Files
# =======================
customers = spark.read.csv("data/customers.csv", header=True, inferSchema=True)
orders = spark.read.csv("data/orders.csv", header=True, inferSchema=True)
products = spark.read.csv("data/products.csv", header=True, inferSchema=True)

print("Sample Customers:")
customers.show(5)
print("Sample Orders:")
orders.show(5)
print("Sample Products:")
products.show(5)

# =======================
# Step 2: Join Data
# =======================
orders_products = orders.join(products, on="product_id", how="left")
orders_customers_products = orders_products.join(customers, on="customer_id", how="left")

# =======================
# Step 3: Transform Data
# =======================
final_df = orders_customers_products.withColumn(
    "total_price",
    round(col("quantity") * col("price_per_unit"), 2)
).withColumn("year", year(col("order_date"))) \
 .withColumn("month", month(col("order_date")))

print("Final Transformed Data:")
final_df.show(10)

# =======================
# Step 4: Write to PostgreSQL
# =======================
final_df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/DataAnalyticsECom") \
    .option("dbtable", "orders_fact") \
    .option("user", "postgres") \
    .option("password", "user") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

print("Data successfully written to PostgreSQL!")

spark.stop()
