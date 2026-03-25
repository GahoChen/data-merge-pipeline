import argparse
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

def create_spark_session():
    """初始化 Spark 环境并配置内存调优参数以防 OOM"""
    return SparkSession.builder \
        .appName("CommercialDataMerge") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

def optimized_inner_join(spark, path_a, path_b, output_path):
    """
    核心合并逻辑：利用 PySpark 读取大规模 CSV 并进行 Inner Join
    针对大小表关联使用 Broadcast 优化，避免全表 Shuffle 引发 OOM
    """
    print("🚀 开始加载数据集...")
    # 读取异构 CSV 数据集
    df_large = spark.read.csv(path_a, header=True, inferSchema=True)
    df_small = spark.read.csv(path_b, header=True, inferSchema=True)

    print("🔄 执行优化的 Inner Join...")
    # 核心技术点：利用 broadcast hint 强制广播小表，极大地提升了合并速度
    merged_df = df_large.join(broadcast(df_small), on="user_id", how="inner")

    print(f"💾 正在将合并结果写入 {output_path}...")
    # 输出为列式存储格式，提高下游读取效率
    merged_df.write.mode("overwrite").parquet(output_path)
    print("✅ 处理流水线执行完毕！")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_a", default="data/table_a.csv", help="主表路径")
    parser.add_argument("--input_b", default="data/table_b.csv", help="维表路径")
    parser.add_argument("--output", default="data/merged_result", help="输出路径")
    args = parser.parse_args()

    spark_env = create_spark_session()
    optimized_inner_join(spark_env, args.input_a, args.input_b, args.output)
