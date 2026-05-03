import sys
from pathlib import Path
from pyspark.sql import functions as F
from pyspark.sql.window import Window

project_root = Path.cwd().resolve()
if not (project_root / "utils").exists():
    project_root = project_root.parent
sys.path.append(str(project_root))

from utils.spark_config import get_spark_session

SILVER_PATH = "/opt/airflow/project/data/silver/cars_delta"

POSTGRES_URL = "jdbc:postgresql://postgres:5432/car_db"

POSTGRES_PROPS = {
    "user": "admin",
    "password": "khanh",
    "driver": "org.postgresql.Driver",
}

def calculate_market_deals(df_silver):
    """1. Market price analysis and label good deals/overpriced cars"""
    df_baseline = df_silver.groupBy(
        "brand", "model", "year", "transmission", "fuel_type"
    ).agg(
        F.percentile_approx("price", 0.5).alias("median_price"),
        F.count("link").alias("total_cars_in_market")
    ).filter(F.col("total_cars_in_market") >= 5) 

    df_deal_finder = df_silver.join(
        F.broadcast(df_baseline),
        on=["brand", "model", "year", "transmission", "fuel_type"],
        how="inner"
    ).withColumn(
        "diff_percentage",
        F.round(((F.col("price") - F.col("median_price")) / F.col("median_price")) * 100, 1)
    ).withColumn(
        "deal_label",
        F.when(F.col("price") <= F.col("median_price") * 0.85, "Great Deal (>15% cheaper)")
         .when(F.col("price") <= F.col("median_price") * 0.95, "Good Price (5-15% cheaper)")
         .when(F.col("price") >= F.col("median_price") * 1.15, "Overpriced (>15% more)")
         .otherwise("Market Price")
    )

    return df_deal_finder.select(
        "link", "location", "brand", "model", "year", "transmission", 
        "price", "median_price", "diff_percentage", "deal_label"
    )

def calculate_regional_trends(df_silver):
    """2. Rank top 3 best-selling models by province/city"""
    df_stats = df_silver.groupBy("location", "brand", "model").agg(
        F.count("link").alias("total_listings"),
        F.percentile_approx("price", 0.5).alias("regional_median_price")
    ).filter(F.col("total_listings") >= 5)

    window_spec = Window.partitionBy("location").orderBy(F.desc("total_listings"))

    df_ranked = df_stats.withColumn(
        "rank_in_region", F.rank().over(window_spec)
    ).filter(F.col("rank_in_region") <= 3)

    return df_ranked.select(
        "location", "rank_in_region", "brand", "model", 
        "total_listings", "regional_median_price"
    )

def calculate_depreciation(df_silver):
    """3. Calculate annual depreciation rate"""
    df_grouped = df_silver.groupBy("brand", "model", "transmission", "fuel_type", "year").agg(
        F.percentile_approx("price", 0.5).alias("median_price"),
        F.count("link").alias("total_cars")
    ).filter(F.col("total_cars") >= 3) 

    window_spec_deprec = Window.partitionBy("brand", "model", "transmission", "fuel_type").orderBy(F.desc("year"))

    df_deprec = df_grouped.withColumn(
        "older_year", F.lead("year", 1).over(window_spec_deprec)
    ).withColumn(
        "older_price", F.lead("median_price", 1).over(window_spec_deprec)
    ).filter(
        F.col("older_year").isNotNull()
    ).withColumn(
        "year_gap", F.col("year") - F.col("older_year")
    ).withColumn(
        "depreciation_per_year_%",
        F.when(
            (F.col("year_gap") > 0) & (F.col("median_price") > 0),
            F.round(((F.col("median_price") - F.col("older_price")) / F.col("median_price") / F.col("year_gap")) * 100, 2)
        ).otherwise(F.lit(0))
    )

    return df_deprec.select(
        "brand", "model", "transmission", "fuel_type", "year", "median_price", 
        "older_year", "older_price", "year_gap", "depreciation_per_year_%"
    )

def calculate_budget_recommendations(df_silver, current_year=2026):
    """4. Budget-based car recommendation system (scoring wear & value)"""
    df_budget = df_silver.withColumn(
        "budget_segment",
        F.when(F.col("price") <= 300000000, "1. Dưới 300 Tr")
         .when((F.col("price") > 300000000) & (F.col("price") <= 500000000), "2. Từ 300 - 500 Tr")
         .when((F.col("price") > 500000000) & (F.col("price") <= 700000000), "3. Từ 500 - 700 Tr")
         .otherwise("4. Trên 700 Tr")
    )

    df_wear_tear = df_budget.withColumn(
        "age", (F.lit(current_year) - F.col("year") + F.lit(1)).cast("int")
    ).withColumn(
        "km_per_year", F.when(F.col("age") > 0, F.col("km_driven") / F.col("age")).otherwise(0)
    ).filter(F.col("km_per_year") < 20000)

    df_scored = df_wear_tear.withColumn(
        "score_year", F.when(F.col("age") >= 10, 0).otherwise(F.round(40 - (F.col("age") * 4), 1))
    ).withColumn(
        "score_usage", F.when(F.col("km_per_year") <= 5000, 40).otherwise(F.round(40 - ((F.col("km_per_year") - 5000) / 15000 * 40), 1))
    ).withColumn(
        "min_price_in_segment", F.min("price").over(Window.partitionBy("budget_segment"))
    ).withColumn(
        "max_price_in_segment", F.max("price").over(Window.partitionBy("budget_segment"))
    ).withColumn(
        "score_value",
        F.when(F.col("max_price_in_segment") == F.col("min_price_in_segment"), 10)
         .otherwise(F.round(20 - ((F.col("price") - F.col("min_price_in_segment")) / (F.col("max_price_in_segment") - F.col("min_price_in_segment")) * 20), 1))
    )

    df_final_score = df_scored.withColumn("total_score", F.col("score_year") + F.col("score_usage") + F.col("score_value"))

    window_recommender = Window.partitionBy("budget_segment").orderBy(F.desc("total_score"))

    df_ranked = df_final_score.withColumn(
        "recommendation_rank", F.row_number().over(window_recommender)
    ).filter(F.col("recommendation_rank") <= 5)

    return df_ranked.select(
        "budget_segment", "recommendation_rank", "link", "location", "brand", "model", 
        "year", "price", "total_score", "score_year", "score_usage", "score_value"
    )

def load_to_postgres(export_tables):
    """Export DataFrames to PostgreSQL"""
    for table_name, dataframe in export_tables.items():
        try:
            (
                dataframe.write
                .format("jdbc")
                .option("url", POSTGRES_URL)
                .option("dbtable", table_name)
                .options(**POSTGRES_PROPS)
                .mode("overwrite")
                .save()
            )
            print(f"[OK] Exported {table_name} to Postgres successfully.")
        except Exception as e:
            print(f"[!] Error exporting table {table_name}: {e}")

def main():
    spark = get_spark_session("Gold_Aggregation_Job")
    
    try:
        print("[*] Reading data from Silver Layer...")
        df_silver = spark.read.format("delta").load(SILVER_PATH)
        
        print("[*] Performing Aggregation & Business Logic...")
        # Create Data Mart tables
        df_gold_deals = calculate_market_deals(df_silver)
        df_gold_regional = calculate_regional_trends(df_silver)
        df_gold_deprec = calculate_depreciation(df_silver)
        
        # Package for export
        export_tables = {
            "gold_deals": df_gold_deals,
            "gold_regional": df_gold_regional,
            "gold_depreciation": df_gold_deprec,
        }
        
        print("Starting data push to PostgreSQL...")
        try:
            load_to_postgres(export_tables)
            print("Gold Job completed!")
        except Exception as e:
            print(f"[!] Error during PostgreSQL export: {e}")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()