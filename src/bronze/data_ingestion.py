import zipfile
import os
from utils.df_utils import create_spark_session, save_to_parquet
from utils.config_utils import config, get_output_dir, get_file_path, get_parquet_path


def extract_file_from_zip(zip_path, output_dir):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(output_dir)
        file_name = zip_ref.infolist()[0].filename
    return os.path.join(output_dir, file_name)

def ingest_data_file(file_path, file_type, spark):
    if file_type == 'json':
        return spark.read.json(file_path)
    elif file_type == 'csv':
        return spark.read.csv(file_path, header=True, inferSchema=True)
    else:
        raise ValueError(f"Unsupported file type: {file_type}")



def ingest_and_save_raw_data():
    bronze_output_dir = get_output_dir('bronze')
    os.makedirs(bronze_output_dir, exist_ok=True)
    
    spark = create_spark_session()
    
    data_dir = os.path.join(config['base_dir'], config['paths']['data_dir'])
    
    # Process rentals data
    rentals_zip = get_file_path('data','zip', 'rentals')
    rentals_path = extract_file_from_zip(rentals_zip, data_dir)
    rentals_df = ingest_data_file(rentals_path, 'json', spark)
    rentals_output_path = get_parquet_path('bronze', 'rentals')
    save_to_parquet(rentals_df, rentals_output_path)

    # Process Airbnb data
    airbnb_zip = get_file_path('data','zip', 'airbnb')
    airbnb_path = extract_file_from_zip(airbnb_zip, data_dir)
    airbnb_df = ingest_data_file(airbnb_path, 'csv', spark)
    airbnb_output_path = get_parquet_path('bronze', 'airbnb')
    save_to_parquet(airbnb_df, airbnb_output_path)
    
    spark.stop()


    

