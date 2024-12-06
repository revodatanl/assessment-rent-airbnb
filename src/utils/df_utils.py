from pyspark.sql import SparkSession
import os
def create_spark_session():
    spark = SparkSession.builder.appName("Amsterdam Properties").getOrCreate()
    return spark

def save_to_parquet(df, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.write.mode('overwrite').parquet(path)
