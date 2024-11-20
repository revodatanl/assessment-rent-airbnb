import unittest
from pyspark.sql import SparkSession
from datetime import date
from silver.rentals_data_cleaning import clean_rentals_data

class TestRentalsCleaning(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[1]").appName("TestRentalsCleaning").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_clean_rentals_data(self):
        data = [
            ("1", "Amsterdam", "Street A", "1000-AB", "A", "Apartment", "45 sqm", "01-01-23 - 31-12-23", 2, False, True, False, "€800,-", "200", "400", "50"),
            ("2", "Rotterdam", "Street B", " 2000 cd", "B", "House", "90sqrm", "01-03-23 - Indefinite period", 1, True, True, True, "€1200.00", "0", "500", "100")
        ]
        columns = ["_id", "city", "title", "postalCode", "energyLabel", "propertyType", "areaSqm", "availability", "roommates",
                   "smokingInside", "shower", "toilet", "rent", "registrationCost", "deposit", "additionalCostsRaw"]
        df = self.spark.createDataFrame(data, columns)

        # Apply cleaning
        cleaned_df = clean_rentals_data(df)

        # Define expected results
        expected_data = [
            ("1", "Amsterdam", "Street A", "1000AB", "A", "Apartment", 45.0, date(2023, 1, 1), date(2023, 12, 31), 2, False, True, False, "800.00", "200.00", "400.00", "50.00"),
            ("2", "Rotterdam", "Street B", "2000CD", "B", "House", 90.0, date(2023, 3, 1), None, 1, True, True, True, "1,200.00", "0.00", "500.00", "100.00")
        ]
        expected_columns = ["_id", "city", "streetAddress", "zipcode", "energyLabel", "propertyType", "areaSqm", 
                            "availability_start_date", "availability_end_date", "roommates", "smokingInside", 
                            "shower", "toilet", "rent", "registrationCost", "deposit", "additionalCostsRaw"]
        
        expected_df = self.spark.createDataFrame(expected_data, expected_columns)
        
        # Collect results and compare
        self.assertEqual(cleaned_df.collect(), expected_df.collect())

    def test_normalize_date_format(self):
        # Test for date cleaning functionality
        pass

if __name__ == "__main__":
    unittest.main()
