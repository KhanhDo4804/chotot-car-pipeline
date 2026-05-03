from delta.tables import DeltaTable
import sys
import os
from pathlib import Path
from pyspark.sql import functions as F

project_root = Path.cwd().resolve()
if not (project_root / "utils").exists():
    project_root = project_root.parent
sys.path.append(str(project_root))

from utils.spark_config import get_spark_session

INPUT_PATH = "/opt/airflow/project/data/bronze/chotot_car_data_new.csv"

TARGET_PATH = "/opt/airflow/project/data/silver/cars_delta"

def remove_non_alnum(col):
    """Clean special characters from title"""
    pattern = r"[^\p{L}\p{N}\s]"
    return F.trim(F.regexp_replace(F.regexp_replace(col, pattern, ""), r"\s+", " "))

def clean_bronze_data(df_raw):
    return (
        df_raw.withColumn("price", F.regexp_replace(F.regexp_replace(F.col("Giá"), r"\.", ""), " đ", "").cast("long"))
        .withColumn("km_driven", F.coalesce(F.col("Số KM đã đi").cast("long"), F.lit(0)))
        .withColumn("location", F.trim(F.element_at(F.split(F.col("Địa chỉ"), ","), -1)))
        .withColumn("clean_title", remove_non_alnum(F.regexp_replace(F.col("Tên"), r"[\r\n]+", " ")))
        .select(
            F.col("Link").alias("link"),
            F.col("clean_title").alias("title"),
            "price",
            "km_driven",
            F.col("Hãng").alias("brand"),
            F.col("Dòng xe").alias("model"),
            F.col("Năm sản xuất").cast("int").alias("year"),
            "location",
            F.col("Hộp số").alias("transmission"),
            F.col("Nhiên liệu").alias("fuel_type"),
        )
        .filter(F.col("location").isNotNull())
        .filter(F.length(F.trim(F.col("location"))) > 0)
        .filter(F.col("link").rlike(r"^https?://"))
        .filter(F.col("price").isNotNull())
        .filter((F.col("price") >= 50000000) & (F.col("price") <= 5000000000))
        .filter(F.col("km_driven").isNotNull())
        .filter(F.col("brand").isNotNull())
        .filter(F.col("model").isNotNull())
        .filter(F.col("year").isNotNull())
        .filter(F.col("km_driven") > 0)
        .filter(F.col("Hộp số").isNotNull())
        .filter(F.col("Nhiên liệu").isNotNull())
        .dropDuplicates(["link"])
    )
def upsert_to_delta(spark, df_source, target_path, join_key="link"):

    if not DeltaTable.isDeltaTable(spark, target_path):
        print(f"No Delta table exists. Initializing at {target_path}...")
        df_source.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(target_path)
        print("New Delta table created")
    else:
        print("Performing CDC data MERGE into Delta Table...")
        dt = DeltaTable.forPath(spark, target_path)
        (
            dt.alias("target")
            .merge(df_source.alias("source"), f"target.{join_key} = source.{join_key}")
            .whenMatchedUpdate(
                set={
                    "price": "source.price",        
                }
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
        print("CDC data MERGE completed!")

def main():
    if not os.path.exists(INPUT_PATH) or os.path.getsize(INPUT_PATH) < 10:
        print("No new data file found or file is empty.")
        return

    spark = get_spark_session("Silver_Processing_CDC_Job")
    
    try:
        df_raw = spark.read.option("header", "true").csv(INPUT_PATH)
        if df_raw.count() == 0: return
        df_cleaned = clean_bronze_data(df_raw) 
        if df_cleaned.count() > 0:
            upsert_to_delta(spark, df_cleaned, TARGET_PATH)
            os.remove(INPUT_PATH)
        else:
            print("Cleaned data is empty.")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()