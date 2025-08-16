from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, count, desc
import matplotlib.pyplot as plt
import os

# Create a folder for plots
os.makedirs("plots", exist_ok=True)

# Spark session
spark = SparkSession.builder \
    .appName("PostgreSQL Analytics with Plots") \
    .config("spark.jars", "file:///C:/Users/asus/Downloads/postgresql-42.2.27.jar") \
    .getOrCreate()

# PostgreSQL connection
url = "jdbc:postgresql://localhost:5432/DataAnalyticsECom"
properties = {
    "user": "postgres",
    "password": "user",
    "driver": "org.postgresql.Driver"
}

# Read table
table_name = "public.orders_fact"
orders_fact_df = spark.read.jdbc(url=url, table=table_name, properties=properties)

print("\n===== SAMPLE DATA =====")
orders_fact_df.show(5)

# 1. Total revenue by region
revenue_by_region = orders_fact_df.groupBy("region").agg(_sum("total_price").alias("total_revenue"))
revenue_pd = revenue_by_region.toPandas()
print("\nRevenue by Region:\n", revenue_pd)

# Plot 1
ax = revenue_pd.plot(kind="bar", x="region", y="total_revenue", legend=False, color="skyblue")
plt.title("Total Revenue by Region")
plt.ylabel("Revenue")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("plots/revenue_by_region.png")
plt.show()

# 2. Top 5 customers by spending
top_customers = orders_fact_df.groupBy("customer_id", "customer_name") \
    .agg(_sum("total_price").alias("customer_spending")) \
    .orderBy(desc("customer_spending")) \
    .limit(5)
top_customers_pd = top_customers.toPandas()
print("\nTop Customers:\n", top_customers_pd)

# Plot 2
ax = top_customers_pd.plot(kind="bar", x="customer_name", y="customer_spending", legend=False, color="orange")
plt.title("Top 5 Customers by Spending")
plt.ylabel("Spending")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("plots/top_customers.png")
plt.show()

# 3. Most popular products
popular_products = orders_fact_df.groupBy("product_id", "product_name") \
    .agg(count("order_id").alias("orders_count")) \
    .orderBy(desc("orders_count")) \
    .limit(5)
popular_products_pd = popular_products.toPandas()
print("\nPopular Products:\n", popular_products_pd)

# Plot 3
ax = popular_products_pd.plot(kind="bar", x="product_name", y="orders_count", legend=False, color="green")
plt.title("Most Popular Products")
plt.ylabel("Orders")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("plots/popular_products.png")
plt.show()

# 4. Monthly revenue trend
monthly_revenue = orders_fact_df.groupBy("year", "month") \
    .agg(_sum("total_price").alias("monthly_revenue")) \
    .orderBy("year", "month")
monthly_revenue_pd = monthly_revenue.toPandas()
print("\nMonthly Revenue Trend:\n", monthly_revenue_pd)

# Plot 4
plt.plot(monthly_revenue_pd["month"], monthly_revenue_pd["monthly_revenue"], marker="o")
plt.title("Monthly Revenue Trend")
plt.xlabel("Month")
plt.ylabel("Revenue")
plt.grid(True)
plt.tight_layout()
plt.savefig("plots/monthly_revenue.png")
plt.show()

# Stop Spark
spark.stop()
