
from utils.df_utils import create_spark_session, save_to_parquet
from silver.airbnb_data_cleaning import enrich_airbnb_zipcode, clean_airbnb_data
from silver.rentals_data_cleaning import clean_rentals_data
from utils.config_utils import get_parquet_path



def load_and_clean_data():
    spark = create_spark_session()
    
    rentals_file = get_parquet_path('bronze', 'rentals')
    rentals_df = spark.read.parquet(rentals_file)
    rentals_df = clean_rentals_data(rentals_df)
    
    rentals_output_path = get_parquet_path('silver', 'rentals')
    save_to_parquet(rentals_df, rentals_output_path)

    
    airbnb_file = get_parquet_path('bronze', 'airbnb')
    airbnb_df = spark.read.parquet(airbnb_file)
    enriched_airbnb_df = enrich_airbnb_zipcode(airbnb_df, spark)
    airbnb_df = clean_airbnb_data(enriched_airbnb_df)
    
    airbnb_output_path = get_parquet_path('silver', 'airbnb')
    save_to_parquet(airbnb_df, airbnb_output_path)

    spark.stop()
    
load_and_clean_data()