# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # nb_olist_silver — Bronze를 정제해 Silver 테이블 만들기
# 
# | 테이블 | 내용 |
# |---|---|
# | silver_orders | 주문 1건 = 1행. 중복 제거, 일시 열을 timestamp로 변환, `purchase_month`(yyyy-MM) 추가 |
# | silver_order_items | 주문 품목 1개 = 1행. silver_orders에 있는 주문만 남기고, 영문 카테고리(`category_en`)를 붙임 |
# 
# 매출 금액은 `price`(배송비 제외), 배송비는 `freight_value`입니다.

# PARAMETERS CELL ********************

env = "Dev"
lakehouse_name = "LH_Olist"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
from pyspark.sql import functions as F

lh = notebookutils.lakehouse.get(lakehouse_name)
ws_id = notebookutils.runtime.context["currentWorkspaceId"]
TABLES = f"abfss://{ws_id}@onelake.dfs.fabric.microsoft.com/{lh.id}/Tables"
# 스키마를 쓰는 Lakehouse(Tables/dbo/...)라면 dbo 스키마 아래에 저장한다
if any(f.name.strip("/") == "dbo" for f in notebookutils.fs.ls(TABLES)):
    TABLES += "/dbo"

def read_table(name):
    return spark.read.format("delta").load(f"{TABLES}/{name}")

def write_table(df, name):
    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true").save(f"{TABLES}/{name}"))
    n = df.count()
    print(f"{name:<25} {n:>8,} rows")
    return n

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1) silver_orders
# edited on GitHub: 주문 ID가 없는 행과 중복 주문을 제거한다
orders = read_table("bronze_orders")

silver_orders = (
    orders
    .filter(F.col("order_id").isNotNull())
    .dropDuplicates(["order_id"])
    .select(
        "order_id",
        "customer_id",
        "order_status",
        F.to_timestamp("order_purchase_timestamp").alias("purchase_ts"),
        F.to_timestamp("order_approved_at").alias("approved_ts"),
        F.to_timestamp("order_delivered_carrier_date").alias("delivered_carrier_ts"),
        F.to_timestamp("order_delivered_customer_date").alias("delivered_customer_ts"),
        F.to_timestamp("order_estimated_delivery_date").alias("estimated_delivery_ts"),
    )
    .withColumn("purchase_month", F.date_format("purchase_ts", "yyyy-MM"))
    .withColumn("_env", F.lit(env))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2) silver_order_items (제품 카테고리 영문명 붙이기)
items = read_table("bronze_order_items")
products = read_table("bronze_products")
translation = read_table("bronze_category_translation")

product_dim = (
    products.select("product_id", "product_category_name")
    .join(translation.select("product_category_name", "product_category_name_english"),
          on="product_category_name", how="left")
    .select(
        "product_id",
        F.coalesce("product_category_name_english", "product_category_name", F.lit("unknown"))
         .alias("category_en"),
    )
)

silver_order_items = (
    items
    .join(silver_orders.select("order_id"), on="order_id", how="inner")   # 적재된 주문만
    .select(
        "order_id",
        F.col("order_item_id").cast("int").alias("order_item_id"),
        "product_id",
        "seller_id",
        F.col("price").cast("double").alias("price"),
        F.col("freight_value").cast("double").alias("freight_value"),
    )
    .join(product_dim, on="product_id", how="left")
    .withColumn("category_en", F.coalesce("category_en", F.lit("unknown")))
    .withColumn("_env", F.lit(env))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 3) 저장
counts = {
    "silver_orders": write_table(silver_orders, "silver_orders"),
    "silver_order_items": write_table(silver_order_items, "silver_order_items"),
}
notebookutils.notebook.exit(json.dumps({"env": env, "layer": "silver", "counts": counts}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
