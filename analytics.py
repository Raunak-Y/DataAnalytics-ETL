from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, count, desc

# Spark session
spark = SparkSession.builder \
    .appName("PostgreSQL Analytics") \
    .config("spark.jars", "file:///C:/Users/asus/Downloads/postgresql-42.2.27.jar") \
    .getOrCreate()

# PostgreSQL connection
url = "jdbc:postgresql://localhost:5432/DataAnalyticsECom"
properties = {
    "user": "postgres",
    "password": "user",
    "driver": "org.postgresql.Driver"
}

# ✅ Ensure schema-qualified table name
table_name = "public.orders_fact"

# Read table into Spark DataFrame
orders_fact_df = spark.read.jdbc(url=url, table=table_name, properties=properties)

print("\n===== SAMPLE DATA =====")
orders_fact_df.show(5)

# 1. Total revenue by region
print("\n===== Total Revenue by Region =====")
revenue_by_region = orders_fact_df.groupBy("region").agg(_sum("total_price").alias("total_revenue"))
revenue_by_region.show()

# 2. Top 5 customers by spending
print("\n===== Top 5 Customers by Spending =====")
top_customers = orders_fact_df.groupBy("customer_id", "customer_name") \
    .agg(_sum("total_price").alias("customer_spending")) \
    .orderBy(desc("customer_spending")) \
    .limit(5)
top_customers.show()

# 3. Most popular products (by order count)
print("\n===== Most Popular Products =====")
popular_products = orders_fact_df.groupBy("product_id", "product_name") \
    .agg(count("order_id").alias("orders_count")) \
    .orderBy(desc("orders_count")) \
    .limit(5)
popular_products.show()

# 4. Monthly revenue trend
print("\n===== Monthly Revenue Trend =====")
monthly_revenue = orders_fact_df.groupBy("year", "month") \
    .agg(_sum("total_price").alias("monthly_revenue")) \
    .orderBy("year", "month")
monthly_revenue.show()

# Stop Spark
spark.stop()
