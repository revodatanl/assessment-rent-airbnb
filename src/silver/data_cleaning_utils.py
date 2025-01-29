from pyspark.sql.functions import  regexp_replace, col, upper


def cast_to_number(num_cols, data_type='FLOAT'):
    return [
        f"CASE WHEN regexp_extract({col}, r'([0-9,\\.]+)', 1) IS NOT NULL "
        f"THEN CAST(REPLACE(regexp_extract({col}, r'([0-9,\\.]+)', 1), ',', '.') AS {data_type}) "
        f"ELSE NULL END AS {col}"
        for col in num_cols
    ]
    
def format_price_columns(price_cols):
    return [f"FORMAT_NUMBER({col}, 2) AS {col}" for col in price_cols]
    
    
def apply_column_expressions(df, cols, expression):
    other_cols =  [col for col in df.columns if col not in cols]
    df = df.selectExpr(*other_cols, *expression)
    return df

def clean_zipcode_column(df, zipcode_col):
    df = df.withColumn('zipcode', regexp_replace(upper(col(zipcode_col)), r'[^0-9A-Z]', ''))
    return df

def drop_na_with_conditions(df, rent_col):
    df = df.dropna(subset=[rent_col,'zipcode'])
    df = df.filter(~col('zipcode').isin('', '0'))
    return df