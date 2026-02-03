# Typically I will include fixtures to create / tier down the necessary test environment,
# in order to be able to do proper unit and component testing on the data.

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(name="spark", scope="session")
def spark_fixture():
    spark = SparkSession.builder.getOrCreate()
    yield spark
    spark.stop()


def prepare_data():
    pass


def data_loader():
    pass


def teardown_data():
    pass
