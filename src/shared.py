from pyspark.sql import DataFrame
from koheesio.spark.transformations.hash import Sha2Hash
from koheesio.spark.transformations.uuid5 import HashUUID5
from koheesio.spark.transformations.row_number_dedup import RowNumberDedup

CATALOG = "development"
SCHEMA = "personal_mmityu"
BASE_VOLUMES_PATH = f"/Volumes/{CATALOG}/{SCHEMA}"

CONFIG = {
    "catalog": "development",
    "schema": "personal_mmityu",
    "lz": {
        "airbnb": "lz_airbnb",
        "rentals": "lz_rentals"
    },
    "raw": {
        "airbnb": "raw_airbnb",
        "rentals": "raw_rentals"
    }
}

def common_transformations(df: DataFrame, hash_key_columns: list, hash_output_column: str) -> DataFrame:
    return (
        df.transform(
            HashUUID5(
                source_columns=hash_key_columns,
                target_column=hash_output_column,
                namespace="revodata",
            )
        )
        # .transform(RowNumberDedup(columns=[hash_output_column], sort_columns=[hash_output_column]))
        .transform(
            Sha2Hash(
                columns=list(set(df.columns)),
                target_column="meta_record_checksum_txt",
            )
        )
    )