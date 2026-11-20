
from pyspark import pipelines as dp
from pyspark.sql.functions import col, to_timestamp

# 1. Clean Orders
@dp.materialized_view(name="silver_orders")
def silver_orders():
    df = spark.read.table("bronze_orders")
    return (
        df.dropDuplicates(["order_id"])
          .withColumn("order_purchase_timestamp",
                      to_timestamp("order_purchase_timestamp"))
          .withColumn("order_delivered_customer_date",
                      to_timestamp("order_delivered_customer_date"))
          .filter(col("order_id").isNotNull())
    )

# 2. Clean Customers
@dp.materialized_view(name="silver_customers")
def silver_customers():
    return (
        spark.read.table("bronze_customers")
        .dropDuplicates(["customer_id"])
        .filter(col("customer_id").isNotNull())
    )

# 3. Clean Order Items
@dp.materialized_view(name="silver_order_items")
def silver_order_items():
    return (
        spark.read.table("bronze_order_items")
        .dropDuplicates(["order_id", "order_item_id"])
        .filter(col("order_id").isNotNull())
        .filter(col("price") >= 0)
    )

# 4. Clean Payments
@dp.materialized_view(name="silver_payments")
def silver_payments():
    return (
        spark.read.table("bronze_payments")
        .dropDuplicates(["order_id", "payment_sequential"])
        .filter(col("order_id").isNotNull())
        .filter(col("payment_value") >= 0)
    )

# 5. Clean Reviews
@dp.materialized_view(name="silver_reviews")
def silver_reviews():
    return (
        spark.read.table("bronze_reviews")
        .dropDuplicates(["review_id"])
        .filter(col("review_id").isNotNull())
    )

# 6. Clean Products
@dp.materialized_view(name="silver_products")
def silver_products():
    return (
        spark.read.table("bronze_products")
        .dropDuplicates(["product_id"])
        .filter(col("product_id").isNotNull())
    )

# 7. Clean Sellers
@dp.materialized_view(name="silver_sellers")
def silver_sellers():
    return (
        spark.read.table("bronze_sellers")
        .dropDuplicates(["seller_id"])
        .filter(col("seller_id").isNotNull())
    )

# 8. Clean Geolocation
@dp.materialized_view(name="silver_geolocation")
def silver_geolocation():
    return (
        spark.read.table("bronze_geolocation")
        .dropDuplicates()
        .filter(col("geolocation_zip_code_prefix").isNotNull())
    )

# 9. Clean Category Translation
@dp.materialized_view(name="silver_category_translation")
def silver_category_translation():
    return (
        spark.read.table("bronze_category_translation")
        .dropDuplicates(["product_category_name"])
        .filter(col("product_category_name").isNotNull())
    )
