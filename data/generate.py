import pandas as pd
import random
from datetime import datetime, timedelta

# ----- Customers -----
customers = []
regions = ["North", "South", "East", "West"]
for cid in range(101, 201):  # 100 customers
    customers.append([
        cid,
        f"Customer_{cid}",
        random.choice(regions)
    ])

customers_df = pd.DataFrame(customers, columns=["customer_id", "customer_name", "region"])
customers_df.to_csv("customers.csv", index=False)


# ----- Products -----
products = [
    [501, "Saree", "Clothing"],
    [502, "Shoes", "Footwear"],
    [503, "Mixer", "Appliances"],
    [504, "T-shirt", "Clothing"],
    [505, "Watch", "Accessories"],
    [506, "Bowl Set", "Kitchen"],
    [507, "Handbag", "Accessories"],
    [508, "Laptop Stand", "Electronics"],
    [509, "Blender", "Appliances"],
    [510, "Sneakers", "Footwear"]
]
products_df = pd.DataFrame(products, columns=["product_id", "product_name", "category"])
products_df.to_csv("products.csv", index=False)


# ----- Orders -----
orders = []
start_date = datetime(2024, 1, 1)
statuses = ["Completed", "Cancelled"]

for oid in range(1, 5001):  # 5000 orders
    cust_id = random.randint(101, 200)
    prod_id = random.randint(501, 510)
    order_dt = start_date + timedelta(days=random.randint(0, 500))
    qty = random.randint(1, 5)
    price = random.choice([150, 200, 300, 400, 500, 700, 1000, 1500])
    status = random.choices(statuses, weights=[0.85, 0.15])[0]  # 85% completed

    orders.append([
        oid, cust_id, prod_id,
        order_dt.strftime("%Y-%m-%d"),
        qty, price, status
    ])

orders_df = pd.DataFrame(orders, columns=[
    "order_id", "customer_id", "product_id", "order_date",
    "quantity", "price_per_unit", "status"
])
orders_df.to_csv("orders.csv", index=False)

print("✅ CSV files created in 'data/' folder:")
print("- customers.csv")
print("- products.csv")
print("- orders.csv")
