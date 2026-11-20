
from pyspark import pipelines as dp

# Location of uploaded Olist CSV files
DATA_PATH = "/Volumes/workspace/default/ecommerce_data/"

# 1. Orders
@dp.table(name="bronze_orders")
def bronze_orders():
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(DATA_PATH + "olist_orders_dataset.csv")
    )

# 2. Customers
@dp.table(name="bronze_customers")
def bronze_customers():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(DATA_PATH + "olist_customers_dataset.csv")
    )

# 3. Order Items
@dp.table(name="bronze_order_items")
def bronze_order_items():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(DATA_PATH + "olist_order_items_dataset.csv")
    )

# 4. Payments
@dp.table(name="bronze_payments")
def bronze_payments():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(DATA_PATH + "olist_order_payments_dataset.csv")
    )

# 5. Reviews
@dp.table(name="bronze_reviews")
def bronze_reviews():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(DATA_PATH + "olist_order_reviews_dataset.csv")
    )

# 6. Products
@dp.table(name="bronze_products")
def bronze_products():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(DATA_PATH + "olist_products_dataset.csv")
    )

# 7. Sellers
@dp.table(name="bronze_sellers")
def bronze_sellers():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(DATA_PATH + "olist_sellers_dataset.csv")
    )

# 8. Geolocation
@dp.table(name="bronze_geolocation")
def bronze_geolocation():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(DATA_PATH + "olist_geolocation_dataset.csv")
    )

# 9. Category Translation
@dp.table(name="bronze_category_translation")
def bronze_category_translation():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(DATA_PATH + "product_category_name_translation.csv")
    )
