"""Silver layer: clean, type-cast, validate, and geo-enrich both datasets."""

from __future__ import annotations

import logging
from collections.abc import Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.column import Column
from pyspark.sql.types import BooleanType

from .io import write_parquet
from .metrics import log_row_count, log_valid_quarantine
from .quality import airbnb_silver_valid, rentals_silver_valid
from .schemas import AMSTERDAM_PC4_MAX, AMSTERDAM_PC4_MIN

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Airbnb silver
# ---------------------------------------------------------------------------

def clean_airbnb_silver(
    spark: SparkSession,
    bronze_df: DataFrame,
    post_codes_geojson_path: str | None = None,
    output_path: str | None = None,
    quarantine_path: str | None = None,
) -> DataFrame:
    """Transform Airbnb bronze → silver."""
    logger.info("Cleaning Airbnb silver")

    # 1. Normalise PC4
    df = bronze_df.withColumn(
        "pc4",
        F.regexp_extract(F.trim(F.col("zipcode")), r"^(\d{4})", 1)
    ).withColumn(
        "zipcode_raw", F.col("zipcode")
    ).withColumn(
        "pc4_source",
        F.when(F.col("pc4") != "", F.lit("raw")).otherwise(F.lit("missing"))
    )

    # 2. Cast numeric columns (try_cast → null on bad input, no exception)
    df = (
        df
        .withColumn("latitude",        F.expr("try_cast(latitude as double)"))
        .withColumn("longitude",       F.expr("try_cast(longitude as double)"))
        .withColumn("accommodates",    F.expr("try_cast(accommodates as int)"))
        .withColumn("bedrooms",        F.expr("try_cast(bedrooms as float)"))
        .withColumn("price_eur_night", F.expr("try_cast(price as double)"))
        .withColumn("review_score",    F.expr("try_cast(review_scores_value as float)"))
    )

    # 3. Filter Amsterdam (PC4 1000–1109) or missing (geo-enrich candidates)
    df = df.filter(
        F.expr("try_cast(pc4 as int)").between(AMSTERDAM_PC4_MIN, AMSTERDAM_PC4_MAX)
        | (F.col("pc4") == "")
        | F.col("pc4").isNull()
    )

    # 4. Geo-enrich missing PC4
    if post_codes_geojson_path:
        df = _geo_enrich_pc4(spark, df, post_codes_geojson_path)

    # 5. Split valid / quarantine
    valid_cond = airbnb_silver_valid()
    valid_df, bad_df = _split_by_condition(df, valid_cond)
    log_valid_quarantine(valid_df, bad_df, "Airbnb silver")

    if quarantine_path:
        write_parquet(bad_df, quarantine_path)

    # 6. Outlier flag (3× IQR)
    valid_df = _flag_outliers(valid_df, "price_eur_night", "is_price_outlier")

    silver_df = valid_df.select(
        "zipcode_raw", "pc4", "pc4_source",
        "latitude", "longitude", "room_type",
        "accommodates", "bedrooms",
        "price_eur_night", "review_score",
        "is_price_outlier",
        "_ingested_at", "_source_file",
    ).withColumn("_layer", F.lit("silver"))

    if output_path:
        write_parquet(silver_df, output_path)

    return silver_df


# ---------------------------------------------------------------------------
# Rentals silver
# ---------------------------------------------------------------------------

def clean_rentals_silver(
    spark: SparkSession,
    bronze_df: DataFrame,
    output_path: str | None = None,
    quarantine_path: str | None = None,
) -> DataFrame:
    """Transform Rentals bronze → silver."""
    logger.info("Cleaning Rentals silver")

    df = bronze_df.filter(F.lower(F.trim(F.col("city"))) == "amsterdam")
    log_row_count(df, "Amsterdam rentals: %d")

    # Parse rent: extract digits only → try_cast (handles empty string → null)
    df = df.withColumn(
        "_rent_digits",
        F.regexp_extract(F.regexp_replace(F.col("rent"), r"[^\d]", ""), r"(\d+)", 1)
    ).withColumn(
        "rent_eur_month",
        F.expr("try_cast(_rent_digits as double)")
    ).drop("_rent_digits")

    # Parse additional costs
    df = df.withColumn(
        "_costs_digits",
        F.regexp_extract(F.regexp_replace(F.col("additionalCostsRaw"), r"\s+", ""), r"(\d+)", 1)
    ).withColumn(
        "additional_costs_eur",
        F.when(
            F.col("_costs_digits").isNull() | (F.col("_costs_digits") == ""),
            F.lit(0.0)
        ).otherwise(F.expr("try_cast(_costs_digits as double)"))
    ).drop("_costs_digits")

    # Parse area
    df = df.withColumn(
        "area_sqm",
        F.expr("try_cast(regexp_extract(areaSqm, '(\\\\d+)', 1) as int)")
    )

    # PC4
    df = df.withColumn(
        "pc4",
        F.regexp_extract(F.trim(F.col("postalCode")), r"^(\d{4})", 1)
    )

    # Cast types
    df = (
        df
        .withColumn("latitude",  F.expr("try_cast(latitude as double)"))
        .withColumn("longitude", F.expr("try_cast(longitude as double)"))
        .withColumn("is_active", F.lower(F.col("isRoomActive")).cast(BooleanType()))
        .withColumnRenamed("_id", "listing_id")
        .withColumnRenamed("propertyType", "property_type")
        .withColumnRenamed("energyLabel", "energy_label")
    )

    # Validate
    valid_cond = rentals_silver_valid()
    valid_df, bad_df = _split_by_condition(df, valid_cond)
    log_valid_quarantine(valid_df, bad_df, "Rentals silver")

    if quarantine_path:
        write_parquet(bad_df, quarantine_path)

    valid_df = _flag_outliers(valid_df, "rent_eur_month", "is_rent_outlier")

    silver_df = valid_df.select(
        "listing_id", "pc4",
        "latitude", "longitude",
        "property_type", "area_sqm",
        "rent_eur_month", "additional_costs_eur",
        "is_active", "furnish", "energy_label",
        "is_rent_outlier",
        "city",
        "_ingested_at", "_source_file",
    ).withColumn("_layer", F.lit("silver"))

    if output_path:
        write_parquet(silver_df, output_path)

    return silver_df


# ---------------------------------------------------------------------------
# Geo-enrichment  (Level 5 stretch goal)
# ---------------------------------------------------------------------------

class Pc4SpatialIndex:
    """Point-in-polygon PC4 lookup. Module-level so Spark can broadcast it."""

    def __init__(self, polygon_pairs: list[tuple[str, object]]):
        from shapely.strtree import STRtree

        self._polygons = polygon_pairs
        self._tree = STRtree([poly for _, poly in polygon_pairs])

    def lookup(self, lat: float | None, lon: float | None) -> str | None:
        from shapely.geometry import Point

        if lat is None or lon is None:
            return None
        pt = Point(lon, lat)
        for idx in self._tree.query(pt):
            pc4, poly = self._polygons[int(idx)]
            try:
                if poly.contains(pt):
                    return pc4
            except Exception:
                pass
        return None


def _geo_enrich_pc4(spark: SparkSession, df: DataFrame, geojson_path: str) -> DataFrame:
    """Assign PC4 to Airbnb listings with missing zipcode via spatial join.

    Only rows missing PC4 are geo-enriched; others pass through unchanged.
    """
    missing_pc4 = (F.col("pc4") == "") | F.col("pc4").isNull()
    has_pc4 = df.filter(~missing_pc4)
    needs_geo = df.filter(missing_pc4)
    lookup_udf = _build_pc4_lookup_udf(spark, geojson_path)
    enriched = _apply_pc4_lookup(needs_geo, lookup_udf)
    return has_pc4.unionByName(enriched)


def _flatten_geo_coords(obj: object) -> list[list[float]]:
    """Extract [lon, lat] coordinate pairs from a GeoJSON geometry tree."""
    coords: list[list[float]] = []
    stack = [obj]
    while stack:
        current = stack.pop()
        if isinstance(current, list) and current and isinstance(current[0], (int, float)):
            coords.append(current)
        elif isinstance(current, list):
            stack.extend(current)
    return coords


def _build_pc4_lookup_udf(spark: SparkSession, geojson_path: str):
    """Build a broadcast spatial lookup UDF from post-code polygons."""
    import json as _json

    with open(geojson_path) as fh:
        geojson = _json.load(fh)

    try:
        from shapely.geometry import shape

        polygons = []
        for feat in geojson["features"]:
            props = feat["properties"]
            pc4 = str(props.get("pc4_code") or props.get("pc4") or props.get("postcode", ""))[:4]
            if pc4:
                try:
                    polygons.append((pc4, shape(feat["geometry"])))
                except Exception:
                    pass

        bc_index = spark.sparkContext.broadcast(Pc4SpatialIndex(polygons))

        @F.udf("string")
        def lookup_pc4_udf(lat, lon):
            return bc_index.value.lookup(lat, lon)

    except ImportError:
        bboxes = []
        for feat in geojson["features"]:
            props = feat["properties"]
            pc4 = str(props.get("pc4_code") or props.get("pc4") or props.get("postcode", ""))[:4]
            if not pc4:
                continue
            coords = _flatten_geo_coords(feat["geometry"]["coordinates"])
            if coords:
                lons = [c[0] for c in coords]
                lats = [c[1] for c in coords]
                bboxes.append((pc4, min(lons), min(lats), max(lons), max(lats)))

        bc_bboxes = spark.sparkContext.broadcast(bboxes)

        @F.udf("string")
        def lookup_pc4_udf(lat, lon):
            if lat is None or lon is None:
                return None
            for pc4, min_lon, min_lat, max_lon, max_lat in bc_bboxes.value:
                if min_lat <= lat <= max_lat and min_lon <= lon <= max_lon:
                    return pc4
            return None

    return lookup_pc4_udf


def _apply_pc4_lookup(df: DataFrame, lookup_udf: Callable) -> DataFrame:
    """Apply a PC4 lookup UDF and merge results into pc4 / pc4_source."""
    return (
        df.withColumn("pc4_geo", lookup_udf(F.col("latitude"), F.col("longitude")))
        .withColumn(
            "pc4",
            F.when((F.col("pc4") == "") | F.col("pc4").isNull(), F.col("pc4_geo"))
            .otherwise(F.col("pc4")),
        )
        .withColumn(
            "pc4_source",
            F.when(F.col("pc4_source") == "missing", F.lit("geo_enriched"))
            .otherwise(F.col("pc4_source")),
        )
        .drop("pc4_geo")
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _split_by_condition(df: DataFrame, valid_cond: Column) -> tuple[DataFrame, DataFrame]:
    """Split a DataFrame into valid and quarantine sets without subtract()."""
    valid_df = df.filter(valid_cond)
    bad_df = df.filter(~valid_cond)
    return valid_df, bad_df


def _flag_outliers(df: DataFrame, value_col: str, flag_col: str) -> DataFrame:
    """Flag rows above 3× IQR using a single approxQuantile pass."""
    q1, q3 = _iqr_bounds(df, value_col)
    iqr = q3 - q1
    return df.withColumn(flag_col, F.col(value_col) > (q3 + 3 * iqr))


def _iqr_bounds(df: DataFrame, col: str) -> tuple[float, float]:
    q = df.approxQuantile(col, [0.25, 0.75], 0.01)
    return q[0], q[1]
