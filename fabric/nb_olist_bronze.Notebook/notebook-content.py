# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # nb_olist_bronze — 원본 CSV를 Bronze 테이블로 적재
# 
# Olist 공식 GitHub 저장소의 CSV 4개를 읽어 Lakehouse의 Bronze Delta 테이블로 저장합니다.
# 
# | 테이블 | 원본 파일 | row_limit 적용 |
# |---|---|---|
# | bronze_orders | olist_orders_dataset.csv | O |
# | bronze_order_items | olist_order_items_dataset.csv | X |
# | bronze_products | olist_products_dataset.csv | X |
# | bronze_category_translation | product_category_name_translation.csv | X |
# 
# - 이 노트북은 **기본 Lakehouse가 없어도** 동작합니다. `lakehouse_name`으로 현재 작업 영역의 Lakehouse를 이름으로 찾습니다.
# - 그래서 Dev·Test·Prod·feature 어느 작업 영역에 있어도, 그 작업 영역의 `LH_Olist`에 씁니다.

# PARAMETERS CELL ********************

# 매개 변수 셀 — 파이프라인이 실행할 때 아래 값이 덮어써집니다.
env = "Dev"
row_limit = 5000          # 0이면 전체 행
source_base_url = "https://raw.githubusercontent.com/olist/work-at-olist-data/master/datasets/"
lakehouse_name = "LH_Olist"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1) 현재 작업 영역의 Lakehouse 경로 찾기 (ID를 코드에 적지 않는다)
import json
from datetime import datetime, timezone

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

lh = notebookutils.lakehouse.get(lakehouse_name)
ws_id = notebookutils.runtime.context["currentWorkspaceId"]
TABLES = f"abfss://{ws_id}@onelake.dfs.fabric.microsoft.com/{lh.id}/Tables"
# 스키마를 쓰는 Lakehouse(Tables/dbo/...)라면 dbo 스키마 아래에 저장한다
if any(f.name.strip("/") == "dbo" for f in notebookutils.fs.ls(TABLES)):
    TABLES += "/dbo"

row_limit = int(row_limit)
print(f"env={env}, row_limit={row_limit}")
print(f"Lakehouse: {lakehouse_name} ({lh.id})")
print(f"Tables path: {TABLES}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2) CSV 4개를 읽어 Bronze Delta 테이블로 저장
SOURCES = {
    # 테이블 이름: (원본 파일 이름, row_limit 적용 여부)
    "bronze_orders": ("olist_orders_dataset.csv", True),
    "bronze_order_items": ("olist_order_items_dataset.csv", False),
    "bronze_products": ("olist_products_dataset.csv", False),
    "bronze_category_translation": ("product_category_name_translation.csv", False),
}

ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
counts = {}

for table_name, (file_name, apply_limit) in SOURCES.items():
    nrows = row_limit if (apply_limit and row_limit > 0) else None
    pdf = pd.read_csv(source_base_url + file_name, dtype=str, nrows=nrows, encoding="utf-8-sig")
    pdf = pdf.astype(object).where(pdf.notna(), None)      # NaN -> None (Spark의 null)

    schema = StructType([StructField(c, StringType(), True) for c in pdf.columns])
    sdf = (spark.createDataFrame(pdf, schema=schema)
           .withColumn("_env", F.lit(env))
           .withColumn("_ingested_at", F.lit(ingested_at)))

    (sdf.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"{TABLES}/{table_name}"))

    counts[table_name] = sdf.count()
    print(f"{table_name:<30} {counts[table_name]:>8,} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 3) 결과를 파이프라인에 돌려준다 (Notebook 활동의 출력 exitValue)
notebookutils.notebook.exit(json.dumps({"env": env, "layer": "bronze", "counts": counts}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
