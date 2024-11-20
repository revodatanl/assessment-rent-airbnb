import unittest
from pyspark.sql import SparkSession
from silver.data_cleaning_utils import cast_to_number, format_price_columns, apply_column_expressions, clean_zipcode_column, drop_na_with_conditions

class TestUtils(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[1]").appName("TestUtils").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
    
    def test_cast_to_number(self):
        num_cols = ["area", "price"]
        expected_expressions = [
            "CASE WHEN regexp_extract(area, r'([0-9,\\.]+)', 1) IS NOT NULL "
            "THEN CAST(REPLACE(regexp_extract(area, r'([0-9,\\.]+)', 1), ',', '.') AS FLOAT) "
            "ELSE NULL END AS area",
            "CASE WHEN regexp_extract(price, r'([0-9,\\.]+)', 1) IS NOT NULL "
            "THEN CAST(REPLACE(regexp_extract(price, r'([0-9,\\.]+)', 1), ',', '.') AS FLOAT) "
            "ELSE NULL END AS price"
        ]
        result = cast_to_number(num_cols)
        self.assertEqual(result, expected_expressions)

    def test_format_price_columns(self):
        price_cols = ["rent", "deposit"]
        expected_expressions = [
            "FORMAT_NUMBER(rent, 2) AS rent",
            "FORMAT_NUMBER(deposit, 2) AS deposit"
        ]
        result = format_price_columns(price_cols)
        self.assertEqual(result, expected_expressions)

    def test_apply_column_expressions(self):
        data = [("Amsterdam", "1000sqm", "€2000.00")]
        columns = ["city", "area", "price"]
        df = self.spark.createDataFrame(data, columns)
        
        num_cols = ["area", "price"]
        expressions = cast_to_number(num_cols, data_type='INT')
        result_df = apply_column_expressions(df, num_cols, expressions)

        result_columns = result_df.columns
        expected_columns = ["city", "area", "price"]
        self.assertEqual(result_columns, expected_columns)
    
    def test_clean_zipcode_column(self):
        data = [("Amsterdam", ".1234|AB=")]
        columns = ["city", "zipcode"]
        df = self.spark.createDataFrame(data, columns)
        
        result_df = clean_zipcode_column(df, "zipcode")
        result = result_df.select("zipcode").collect()[0][0]
        
        self.assertEqual(result, "1234AB")

    def test_drop_na_with_conditions(self):
        data = [
            ("Amsterdam", "1000ab", None),
            ("Rotterdam", "", "2000"),
            ("Utrecht", "1234 CD", "1500"),
        ]
        columns = ["city", "zipcode", "rent"]
        df = self.spark.createDataFrame(data, columns)
        
        result_df = drop_na_with_conditions(df, "rent")
        result = result_df.select("city").collect()
        expected_result = [("Utrecht",)]
        
        self.assertEqual(result, expected_result)

if __name__ == "__main__":
    unittest.main()
