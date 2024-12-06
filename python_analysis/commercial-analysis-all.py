import os
import io
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, sum as _sum, mean as _mean, row_number
from pyspark.sql.window import Window

os.environ["PYTHONIOENCODING"] = "utf-8"
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

def recommend_top_region_per_province(spark_df, input_example):
    group_data = spark_df.groupBy(
        "시도명", "행정동명", "상권업종중분류명", "상권업종소분류명"
    ).count()

    middle_category_counts = group_data.groupBy(
        "시도명", "행정동명", "상권업종중분류명"
    ).agg(_sum("count").alias("middle_category_count"))

    merged_data = group_data.join(
        middle_category_counts,
        on=["시도명", "행정동명", "상권업종중분류명"],
        how="inner"
    )

    subcategory_data = merged_data.filter(col("상권업종소분류명") == input_example)

    subcategory_data = subcategory_data.withColumn(
        "subcategory_ratio", col("count") / col("middle_category_count")
    )

    average_middle_count = subcategory_data.select(
        _mean("middle_category_count").alias("avg_middle_count")
    ).first()["avg_middle_count"]

    subcategory_data = subcategory_data.withColumn(
        "weighted_middle_category", col("middle_category_count") / average_middle_count
    ).withColumn(
       "combined_score", col("subcategory_ratio") + col("weighted_middle_category")
    )

    window_spec = Window.partitionBy("시도명").orderBy(col("combined_score").desc())
    top_region_per_province = subcategory_data.withColumn(
        "rank", row_number().over(window_spec)
    ).filter(col("rank") == 1)

    return top_region_per_province.drop("rank")

if __name__ == "__main__":
    conf = SparkConf().setAppName("RegionRecommendation").setMaster("local")
    sc = SparkContext(conf=conf)
    sql_context = SQLContext(sc)
    hdfs_path = "hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/team8/preprocessing/preprocessed_data/part-00000-defb23d5-7721-484f-a98d-7b851d263640-c000.snappy.parquet"
    spark_df = sql_context.read.parquet(hdfs_path)
    input_example = "백반/한정식"
    result_df = recommend_top_region_per_province(spark_df, input_example)

    result_df.show(truncate=False)
