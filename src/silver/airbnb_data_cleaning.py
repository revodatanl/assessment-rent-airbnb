import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
from silver.data_cleaning_utils import apply_column_expressions, clean_zipcode_column, cast_to_number, format_price_columns, drop_na_with_conditions
from utils.config_utils import config
import os

def enrich_airbnb_zipcode(airbnb_df, spark):
    airbnb_pd_df = airbnb_df.toPandas()
    
    airbnb_pd_df['longitude'] = pd.to_numeric(airbnb_pd_df['longitude'], errors='coerce')
    airbnb_pd_df['latitude'] = pd.to_numeric(airbnb_pd_df['latitude'], errors='coerce')
    
    airbnb_pd_df['geometry'] = airbnb_pd_df.apply(lambda row: Point(row['longitude'], row['latitude']), axis=1)
    airbnb_gdf = gpd.GeoDataFrame(airbnb_pd_df, geometry='geometry', crs="EPSG:4326")
    airbnb_gdf = airbnb_gdf.to_crs("EPSG:28992")

    data_dir = os.path.join(config['base_dir'], config['paths']['data_dir'])

    post_codes_file_path = os.path.join(data_dir, config['files']['geojson']['post_codes'])
    
    post_codes = gpd.read_file(post_codes_file_path).to_crs("EPSG:28992")
    airbnb_with_postcode = gpd.sjoin(airbnb_gdf, post_codes[['pc4_code', 'geometry', 'gem_name', 'prov_name']], how='left', predicate='within')
    airbnb_with_postcode['zipcodeEnhanced'] = airbnb_with_postcode['zipcode'].fillna(airbnb_with_postcode['pc4_code'])
    airbnb_with_postcode.drop(columns=['geometry', 'index_right', 'zipcode'], inplace=True)
    
    enriched_df = spark.createDataFrame(airbnb_with_postcode)
    return enriched_df

def clean_airbnb_data(df):

    df = clean_zipcode_column(df, 'zipcodeEnhanced')
    df = drop_na_with_conditions(df, 'price')
    
    df = df.withColumnRenamed('gem_name', 'municipality'
        ).withColumnRenamed('prov_name', 'province')
    
    int_num_cols = config['columns']['airbnb']['int_numbers']
    price_cols = config['columns']['airbnb']['prices']
    
    df = apply_column_expressions(df, int_num_cols, cast_to_number(int_num_cols,'INT'))
    df = apply_column_expressions(df, price_cols, cast_to_number(price_cols, 'FLOAT'))
    df = apply_column_expressions(df, price_cols, format_price_columns(price_cols))
    
    cols = config['columns']['airbnb']['selected']

    df = df.select(cols)
    return df
