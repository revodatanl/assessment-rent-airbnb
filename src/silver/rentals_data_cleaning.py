from silver.data_cleaning_utils import apply_column_expressions, clean_zipcode_column, format_price_columns, cast_to_number,drop_na_with_conditions
from pyspark.sql.functions import col, regexp_replace, to_date, split, when
from utils.config_utils import config

def normalize_date_format(date_str):
    cleaned_date = regexp_replace(date_str, "'", "") 
    cleaned_date = to_date(cleaned_date, 'dd-MM-yy') 
    return  cleaned_date

def clean_rentals_data(df):
    df = clean_zipcode_column(df, 'postalCode')
    df = drop_na_with_conditions(df, 'rent')
    
    num_cols = config['columns']['rentals']['numbers']
    price_cols = config['columns']['rentals']['prices']
    
    df = apply_column_expressions(df, num_cols, cast_to_number(num_cols))
    df = apply_column_expressions(df, price_cols, format_price_columns(price_cols))
    
    df = df.withColumn('dates', split(df['availability'], ' - ')
        ).withColumn('availability_start_date', normalize_date_format(col('dates')[0])
        ).withColumn('availability_end_date', 
                    when(col('dates')[1] == "Indefinite period", None)
                    .otherwise(normalize_date_format(col('dates')[1]))
        ).withColumnRenamed('title','streetAddress')
                    

    cols = config['columns']['rentals']['selected']
    df = df.select(cols)
    
    return df
