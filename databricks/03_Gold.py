from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(name="gold_sales_summary")
def gold_sales_summary():
    orders = spark.read.table("silver_orders")
    items = spark.read.table("silver_order_items")

    order_sales = (
        items.groupBy("order_id")
        .agg(
            F.sum("price").alias("product_revenue"),
            F.sum("freight_value").alias("freight_revenue"),
            F.count("*").alias("total_items")
        )
    )

    return (
        orders.join(order_sales, "order_id", "inner")
        .groupBy(
            F.year("order_purchase_timestamp").alias("year"),
            F.month("order_purchase_timestamp").alias("month")
        )
        .agg(
            F.sum("product_revenue").alias("total_revenue"),
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("total_items").alias("total_items"),
            F.avg("product_revenue").alias("average_order_value")
        )
        .orderBy("year", "month")
    )


@dp.materialized_view(name="gold_customer_summary")
def gold_customer_summary():
    orders = spark.read.table("silver_orders")
    customers = spark.read.table("silver_customers")
    items = spark.read.table("silver_order_items")

    order_revenue = (
        items.groupBy("order_id")
        .agg(F.sum("price").alias("order_revenue"))
    )

    customer_orders = (
        orders.join(customers, "customer_id", "inner")
        .join(order_revenue, "order_id", "left")
    )

    return (
        customer_orders
        .groupBy("customer_unique_id", "customer_state")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("order_revenue").alias("total_spent"),
            F.min("order_purchase_timestamp").alias("first_purchase"),
            F.max("order_purchase_timestamp").alias("last_purchase")
        )
        .withColumn(
            "customer_type",
            F.when(F.col("total_orders") > 1, "Returning")
             .otherwise("One-time")
        )
    )



@dp.materialized_view(name="gold_product_summary")
def gold_product_summary():
    items = spark.read.table("silver_order_items")
    products = spark.read.table("silver_products")
    categories = spark.read.table("silver_category_translation")

    product_data = (
        items.join(products, "product_id", "left")
        .join(categories, "product_category_name", "left")
    )

    return (
        product_data
        .groupBy(
            "product_id",
            "product_category_name_english"
        )
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.count("*").alias("units_sold"),
            F.sum("price").alias("total_revenue"),
            F.avg("price").alias("average_price")
        )
        .orderBy(F.desc("total_revenue"))
    )


@dp.materialized_view(name="gold_delivery_summary")
def gold_delivery_summary():
    orders = spark.read.table("silver_orders")

    delivery_data = (
        orders
        .filter(F.col("order_delivered_customer_date").isNotNull())
        .withColumn(
            "delivery_days",
            F.datediff(
                F.to_date("order_delivered_customer_date"),
                F.to_date("order_purchase_timestamp")
            )
        )
        .withColumn(
            "is_late",
            F.when(
                F.to_date("order_delivered_customer_date") >
                F.to_date("order_estimated_delivery_date"),
                1
            ).otherwise(0)
        )
    )

    return (
        delivery_data
        .groupBy("order_status")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.round(F.avg("delivery_days"), 2).alias("avg_delivery_days"),
            F.sum("is_late").alias("late_deliveries"),
            F.round(F.avg("is_late") * 100, 2).alias("late_delivery_percentage")
        )
    )


@dp.materialized_view(name="gold_payment_review_summary")
def gold_payment_review_summary():
    payments = spark.read.table("silver_payments")
    reviews = spark.read.table("silver_reviews")

    payment_summary = (
        payments
        .groupBy("payment_type")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("payment_value").alias("total_payment_value"),
            F.avg("payment_value").alias("average_payment")
        )
    )

    review_summary = (
        reviews
        .groupBy("review_score")
        .agg(
            F.count("*").alias("total_reviews")
        )
    )

    return payment_summary


@dp.materialized_view(name="gold_review_summary")
def gold_review_summary():
    reviews = spark.read.table("silver_reviews")

    return (
        reviews
        .groupBy("review_score")
        .agg(
            F.count("*").alias("total_reviews"),
            F.round(F.avg("review_score"), 2).alias("average_score")
        )
        .orderBy("review_score")
    )


# ==========================================
# GOLD TABLE 7: SELLER PERFORMANCE
# ==========================================

@dp.materialized_view(name="gold_seller_summary")
def gold_seller_summary():
    items = spark.read.table("silver_order_items")
    sellers = spark.read.table("silver_sellers")

    seller_data = items.join(sellers, "seller_id", "left")

    return (
        seller_data
        .groupBy("seller_id", "seller_state")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.count("*").alias("units_sold"),
            F.round(F.sum("price"), 2).alias("total_revenue"),
            F.round(F.avg("price"), 2).alias("average_item_price"),
            F.round(F.avg("freight_value"), 2).alias("average_freight")
        )
    )


# ==========================================
# GOLD TABLE 8: CUSTOMER GEOGRAPHY
# ==========================================

@dp.materialized_view(name="gold_customer_geography")
def gold_customer_geography():
    orders = spark.read.table("silver_orders")
    customers = spark.read.table("silver_customers")
    items = spark.read.table("silver_order_items")

    order_revenue = (
        items.groupBy("order_id")
        .agg(F.sum("price").alias("order_revenue"))
    )

    customer_data = (
        orders
        .join(customers, "customer_id", "inner")
        .join(order_revenue, "order_id", "left")
    )

    return (
        customer_data
        .groupBy("customer_state", "customer_city")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.countDistinct("customer_unique_id").alias("unique_customers"),
            F.round(F.sum("order_revenue"), 2).alias("total_revenue")
        )
    )


# ==========================================
# GOLD TABLE 9: MONTHLY ORDER STATUS
# ==========================================

@dp.materialized_view(name="gold_monthly_order_status")
def gold_monthly_order_status():
    orders = spark.read.table("silver_orders")

    return (
        orders
        .groupBy(
            F.year("order_purchase_timestamp").alias("year"),
            F.month("order_purchase_timestamp").alias("month"),
            "order_status"
        )
        .agg(
            F.countDistinct("order_id").alias("total_orders")
        )
        .orderBy("year", "month")
    )







