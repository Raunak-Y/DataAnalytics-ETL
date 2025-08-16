from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count, desc
import matplotlib.pyplot as plt

# Start Spark
spark = SparkSession.builder \
    .appName("Retail Analytics") \
    .config("spark.jars", "file:///C:/Users/asus/Downloads/postgresql-42.2.27.jar") \
    .getOrCreate()

url = "jdbc:postgresql://localhost:5432/DataAnalyticsECom"
properties = {
    "user": "postgres",
    "password": "user",   # replace with your actual password
    "driver": "org.postgresql.Driver"
}

# Load fact table
orders_fact_df = spark.read.jdbc(url=url, table="orders_fact", properties=properties)

# --------- ANALYTICS ---------
# 1. Total revenue by region
revenue_by_region = orders_fact_df.groupBy("region") \
    .agg(_sum("total_price").alias("total_revenue")) \
    .orderBy(desc("total_revenue"))

print("Revenue by Region:")
revenue_by_region.show()

# 2. Top 5 products by revenue
top_products = orders_fact_df.groupBy("product_name") \
    .agg(_sum("total_price").alias("revenue")) \
    .orderBy(desc("revenue")) \
    .limit(5)

print("Top 5 Products by Revenue:")
top_products.show()

# 3. Monthly sales trend
monthly_sales = orders_fact_df.groupBy("year", "month") \
    .agg(_sum("total_price").alias("monthly_revenue")) \
    .orderBy("year", "month")

print("Monthly Sales Trend:")
monthly_sales.show()

# --------- VISUALIZATION ---------
# Convert to Pandas for matplotlib
monthly_sales_pd = monthly_sales.toPandas()

plt.figure(figsize=(10,6))
plt.plot(monthly_sales_pd["month"], monthly_sales_pd["monthly_revenue"], marker="o")
plt.title("Monthly Revenue Trend")
plt.xlabel("Month")
plt.ylabel("Revenue")
plt.grid(True)
plt.show()
