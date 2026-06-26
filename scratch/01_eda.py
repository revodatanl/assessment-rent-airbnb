# %% [markdown]
# # EDA: Rent vs Airbnb Amsterdam
# Scratch notebook — data validation checks before building the pipeline.
# Not production code; lives in `scratch/`.

# %%
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import json, re
from collections import Counter

DATA = REPO_ROOT / "data" / "input"

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("eda")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# %% [markdown]
# ## 1. Airbnb CSV

# %%
airbnb_raw = spark.read.option("header", "true").csv(str(DATA / "airbnb.csv"))
airbnb_raw.printSchema()
airbnb_raw.show(5)

# %%
print("Total rows:", airbnb_raw.count())

# Zipcode quality
total = airbnb_raw.count()
missing_zip = airbnb_raw.filter(F.col("zipcode").isNull() | (F.trim(F.col("zipcode")) == "")).count()
print(f"Missing zipcode: {missing_zip} / {total} ({missing_zip/total*100:.1f}%)")

# Zipcode format breakdown
from pyspark.sql.functions import regexp_extract, length, trim

airbnb_raw = airbnb_raw.withColumn("zip_clean", trim(F.col("zipcode")))
four_digit  = airbnb_raw.filter(F.col("zip_clean").rlike(r"^\d{4}$")).count()
with_suffix = airbnb_raw.filter(F.col("zip_clean").rlike(r"^\d{4}\s*[A-Z]{2}$")).count()
print(f"4-digit clean: {four_digit}, with letter suffix: {with_suffix}")

# %%
# Price distribution
airbnb_raw.select(
    F.col("price").cast("double").alias("price")
).agg(
    F.min("price").alias("min"),
    F.percentile_approx("price", 0.25).alias("p25"),
    F.percentile_approx("price", 0.50).alias("median"),
    F.percentile_approx("price", 0.75).alias("p75"),
    F.max("price").alias("max"),
    F.avg("price").alias("avg"),
    F.count("price").alias("count_non_null"),
).show()

# %%
# Room type breakdown
airbnb_raw.groupBy("room_type").count().orderBy(F.desc("count")).show()

# %%
# How many listings missing BOTH zipcode and lat/lon?
both_missing = airbnb_raw.filter(
    (F.col("zipcode").isNull() | (F.trim(F.col("zipcode")) == ""))
    & (F.col("latitude").isNull() | (F.col("longitude").isNull()))
).count()
print(f"Missing zipcode AND coordinates: {both_missing}")

# Can recover from lat/lon: geo-enrichment candidates
geo_recoverable = airbnb_raw.filter(
    (F.col("zipcode").isNull() | (F.trim(F.col("zipcode")) == ""))
    & F.col("latitude").isNotNull()
    & F.col("longitude").isNotNull()
).count()
print(f"Missing zipcode but have lat/lon (geo-enrichable): {geo_recoverable}")

# %% [markdown]
# ## 2. Rentals JSON (Kamernet)

# %%
with open(DATA / "rentals.json") as f:
    records = json.load(f)

print(f"Total records: {len(records)}")
print(f"Fields: {list(records[0].keys())}")

# City breakdown
cities = Counter(r.get("city", "") for r in records)
print("\nTop cities:")
for city, count in cities.most_common(10):
    print(f"  {city}: {count}")

# %%
ams = [r for r in records if r.get("city", "").lower() == "amsterdam"]
print(f"\nAmsterdam records: {len(ams)}")

# Property types in Amsterdam
ptypes = Counter(r.get("propertyType") for r in ams)
print("Property types:", dict(ptypes))

# %%
# Rent parsing
def parse_rent(r):
    s = r.get("rent", "")
    s = s.replace(".", "").replace(",", ".")
    m = re.search(r"[\d]+(?:\.\d+)?", s)
    return float(m.group()) if m else None

rents = [parse_rent(r) for r in ams if parse_rent(r)]
print(f"Rent — min: €{min(rents):.0f}, max: €{max(rents):.0f}, avg: €{sum(rents)/len(rents):.0f}")

# Suspicious outliers
print(f"Rent < €200: {sum(1 for r in rents if r < 200)} records")
print(f"Rent > €3000: {sum(1 for r in rents if r > 3000)} records")

# %%
# Postal code quality in Amsterdam
pc_missing = sum(1 for r in ams if not r.get("postalCode"))
print(f"Missing postalCode (Amsterdam): {pc_missing} / {len(ams)}")

# PC4 range check
pc4s = [r.get("postalCode", "")[:4] for r in ams if r.get("postalCode")]
non_ams = [p for p in pc4s if p.isdigit() and not (1000 <= int(p) <= 1109)]
print(f"PC4 outside Amsterdam range (1000-1109): {len(non_ams)} — {set(non_ams)}")

# %% [markdown]
# ## 3. Geo data

# %%
with open(DATA / "geo/post_codes.geojson") as f:
    pc_geo = json.load(f)

features = pc_geo["features"]
print(f"Postcode polygons: {len(features)}")
print("Sample properties:", features[0]["properties"])

# %%
with open(DATA / "geo/amsterdam_areas.geojson") as f:
    areas_geo = json.load(f)

print(f"Amsterdam area polygons: {len(areas_geo['features'])}")
print("Sample properties:", areas_geo["features"][0]["properties"])

# %% [markdown]
# ## 4. Data quality summary
#
# | Issue | Airbnb | Rentals |
# |---|---|---|
# | Missing zipcode/postcode | 2,254 (22.7%) | 0 (0%) |
# | Geo-recoverable (lat/lon present) | ~2,254 | n/a |
# | Price < floor / > cap | ~13 listings (price > €5k) | ~minimal |
# | Non-Amsterdam records | filtered by PC4 range | filtered by city |
# | Array-wrapped scalar fields | n/a | _id, *At fields |
#
# **Backfill strategy for missing Airbnb zipcodes:**
# 1. For listings with lat/lon: spatial join against `post_codes.geojson` → assign PC4
# 2. For listings without lat/lon: cannot recover — quarantine
# 3. Re-run pipeline when Airbnb provides updated data
