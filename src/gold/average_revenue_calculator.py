
from utils.df_utils import create_spark_session, save_to_parquet
from utils.config_utils import config, get_parquet_path
from pyspark.sql.functions import col, round, regexp_replace, format_number, avg

def calculate_avg_revenue(df, _type):
    average_revenue_col = config['columns'][_type]['average_revenue']
    revenue_col =  config['columns'][_type]['revenue']
    
    df = df.withColumn(revenue_col, regexp_replace(revenue_col, ",", "").cast("float"))
    
    avg_rev = df.groupBy('zipcode').agg(avg(revenue_col).alias(average_revenue_col))
    avg_rev = avg_rev.withColumn(average_revenue_col, round(col(average_revenue_col), 2))\
        .withColumn(average_revenue_col, format_number(average_revenue_col,2))
    return avg_rev


def calculate_all_avg_revenue():
    spark = create_spark_session()

    cleaned_rentals_df = spark.read.parquet(get_parquet_path('silver', 'rentals'))
    cleaned_airbnb_df = spark.read.parquet(get_parquet_path('silver', 'airbnb'))
    
    rentals_rev = calculate_avg_revenue(cleaned_rentals_df, 'rentals')
    airbnb_rev = calculate_avg_revenue(cleaned_airbnb_df, 'airbnb')
    
    all_avg_rev = rentals_rev.join(airbnb_rev, on='zipcode', how='fullouter')
    output_path = get_parquet_path('gold', 'output')
    save_to_parquet(all_avg_rev, output_path)
    
    spark.stop()
