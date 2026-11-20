# Databricks notebook source

# SMART E-COMMERCE ANALYTICS
# GOLD LAYER VALIDATION

gold_tables = [
    "gold_sales_summary",
    "gold_customer_summary",
    "gold_product_summary",
    "gold_delivery_summary",
    "gold_payment_review_summary",
    "gold_review_summary",
    "gold_seller_summary",
    "gold_customer_geography",
    "gold_monthly_order_status"
]

print("=" * 55)
print("GOLD LAYER DATA VALIDATION")
print("=" * 55)

for table in gold_tables:
    try:
        df = spark.table(f"workspace.default.{table}")
        total_rows = df.count()

        if total_rows > 0:
            print(f"SUCCESS: {table} | Rows: {total_rows}")
        else:
            print(f"WARNING: {table} is empty")

    except Exception as e:
        print(f"FAILED: {table}")
        print(str(e))

print("=" * 55)
print("VALIDATION COMPLETED")
print("=" * 55)
