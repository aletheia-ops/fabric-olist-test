# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # nb_olist_gold — 분석용 Gold 테이블 만들기
# 
# | 테이블 | 1행의 의미 | 주요 열 |
# |---|---|---|
# | gold_monthly_sales | 구매 월 | order_count, item_count, revenue, freight, avg_order_value |
# | gold_category_sales | 제품 카테고리 | order_count, item_count, revenue, avg_item_price |
# | gold_order_status | 주문 상태 | order_count, share_pct |
# 
# 매출 집계에서는 `canceled`, `unavailable` 상태의 주문을 뺍니다.
# 
# 담당자: XXX | 최종 수정: k20 실습

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

orders = read_table("silver_orders")
items = read_table("silver_order_items")

EXCLUDED_STATUS = ["canceled", "unavailable"]
sales = (items.join(orders.select("order_id", "order_status", "purchase_month"), "order_id")
              .filter(~F.col("order_status").isin(EXCLUDED_STATUS)))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1) 월별 매출
gold_monthly_sales = (
    sales.groupBy("purchase_month")
    .agg(
        F.countDistinct("order_id").alias("order_count"),
        F.count("*").alias("item_count"),
        F.round(F.sum("price"), 2).alias("revenue"),
        F.round(F.sum("freight_value"), 2).alias("freight"),
    )
    .withColumn("avg_order_value", F.round(F.col("revenue") / F.col("order_count"), 2))
    .withColumn("_env", F.lit(env))
    .orderBy("purchase_month")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2) 카테고리별 매출
gold_category_sales = (
    sales.groupBy("category_en")
    .agg(
        F.countDistinct("order_id").alias("order_count"),
        F.count("*").alias("item_count"),
        F.round(F.sum("price"), 2).alias("revenue"),
        F.round(F.avg("price"), 2).alias("avg_item_price"),
    )
    .withColumn("_env", F.lit(env))
    .orderBy(F.desc("revenue"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 3) 주문 상태 분포 (모든 상태 포함)
total_orders = orders.count()
gold_order_status = (
    orders.groupBy("order_status")
    .agg(F.count("*").alias("order_count"))
    .withColumn("share_pct", F.round(F.col("order_count") * 100.0 / F.lit(total_orders), 2))
    .withColumn("_env", F.lit(env))
    .orderBy(F.desc("order_count"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 5) 판매자별 매출 — feature 브랜치에서 추가한 기능
gold_seller_sales = (
    sales.groupBy("seller_id")
    .agg(
        F.countDistinct("order_id").alias("order_count"),
        F.count("*").alias("item_count"),
        F.round(F.sum("price"), 2).alias("revenue"),
    )
    .withColumn("_env", F.lit(env))
    .orderBy(F.desc("revenue"))
)
display(gold_seller_sales.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 4) 저장하고 결과 요약을 파이프라인에 돌려준다
counts = {
    "gold_monthly_sales": write_table(gold_monthly_sales, "gold_monthly_sales"),
    "gold_category_sales": write_table(gold_category_sales, "gold_category_sales"),
    "gold_order_status": write_table(gold_order_status, "gold_order_status"),
    "gold_seller_sales": write_table(gold_seller_sales, "gold_seller_sales"), # ← 추가
}
total_revenue = gold_monthly_sales.agg(F.round(F.sum("revenue"), 2)).first()[0]
print(f"total revenue (BRL): {total_revenue:,.2f}")

notebookutils.notebook.exit(json.dumps(
    {"env": env, "layer": "gold", "counts": counts, "total_revenue": total_revenue}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
