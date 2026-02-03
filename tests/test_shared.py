import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, StringType, StructField, StructType

from src.shared import common_transformations


@pytest.mark.unit
class TestShared:
    @pytest.fixture(scope="class")
    def df(self, spark):
        schema = StructType(
            [
                StructField("col_a", StringType(), True),
                StructField("col_b", StringType(), True),
                StructField("col_c", FloatType(), True),
            ]
        )

        data = [
            ("foo", "bar", 1.23),
            ("baz", "qux", 4.56),
        ]

        df = spark.createDataFrame(data, schema)
        yield df

    def test_common_transformations(self, df):
        df = common_transformations(df, ["col_a"], "col_uuid")

        assert "col_uuid" in df.columns
        assert "meta_record_checksum_txt" in df.columns
