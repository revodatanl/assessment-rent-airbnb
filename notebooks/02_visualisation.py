# %% [markdown]
# # Revenue by Postcode — Airbnb vs Kamernet
# Visualises gold-layer output: average annual revenue per PC4.
# Requires pipeline output at `data/output/gold/investment_comparison/`.
#
# Run after: `python -m rent_airbnb.pipeline` (from repo root)

# %%
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

import matplotlib.pyplot as plt
from pyspark.sql import functions as F

from rent_airbnb.pipeline import PipelineConfig, get_or_create_spark

config = PipelineConfig(output_base=str(REPO_ROOT / "data" / "output"))
spark = get_or_create_spark(config)

comparison_path = config.gold_comparison_path
if not Path(comparison_path).exists():
    raise FileNotFoundError(
        f"Gold output not found at {comparison_path}. "
        "Run the pipeline first: python -m rent_airbnb.pipeline"
    )

comparison = spark.read.parquet(comparison_path)
top_n = 20

plot_df = (
    comparison.select(
        "pc4",
        "airbnb_avg_annual_revenue_eur",
        "rental_avg_annual_revenue_eur",
        "recommendation",
    )
    .filter(F.col("airbnb_avg_annual_revenue_eur").isNotNull())
    .orderBy(F.desc("airbnb_avg_annual_revenue_eur"))
    .limit(top_n)
    .toPandas()
)

# %% [markdown]
# ## Bar chart — top PC4s by Airbnb revenue

# %%
fig, ax = plt.subplots(figsize=(12, 6))
x = range(len(plot_df))
width = 0.35

ax.bar(
    [i - width / 2 for i in x],
    plot_df["airbnb_avg_annual_revenue_eur"],
    width,
    label="Airbnb",
    color="#FF5A5F",
)
ax.bar(
    [i + width / 2 for i in x],
    plot_df["rental_avg_annual_revenue_eur"].fillna(0),
    width,
    label="Kamernet rental",
    color="#2E86AB",
)

ax.set_xticks(list(x))
ax.set_xticklabels(plot_df["pc4"])
ax.set_xlabel("Postcode (PC4)")
ax.set_ylabel("Avg annual revenue (EUR)")
ax.set_title(f"Top {top_n} Amsterdam PC4s — Airbnb vs long-term rental")
ax.legend()
ax.grid(axis="y", alpha=0.3)
plt.tight_layout()
plt.show()

# %% [markdown]
# ## Recommendation breakdown (all PC4s)

# %%
rec_counts = (
    comparison.groupBy("recommendation")
    .count()
    .orderBy(F.desc("count"))
    .toPandas()
)

fig, ax = plt.subplots(figsize=(7, 4))
ax.bar(rec_counts["recommendation"], rec_counts["count"], color="#6C757D")
ax.set_xlabel("Recommendation")
ax.set_ylabel("Number of PC4 areas")
ax.set_title("Investment recommendation by postcode area")
plt.tight_layout()
plt.show()

# %% [markdown]
# ## Map — Airbnb revenue by postcode polygon
# Uses `data/input/geo/post_codes.geojson` for PC4 boundaries.

# %%
import json
from pathlib import Path

from matplotlib.collections import PatchCollection
from matplotlib.patches import Polygon as MplPolygon
from matplotlib.colors import Normalize
from matplotlib.cm import ScalarMappable

geo_path = REPO_ROOT / "data/input/geo/post_codes.geojson"
pc4_revenue = {
    row["pc4"]: row["airbnb_avg_annual_revenue_eur"]
    for row in comparison.select("pc4", "airbnb_avg_annual_revenue_eur").collect()
    if row["pc4"]
}

with open(geo_path) as fh:
    geojson = json.load(fh)


def _pc4_revenue_patches(geo: dict, revenue_by_pc4: dict) -> tuple[list, list]:
    """Build fresh polygon patches per figure (matplotlib artists are single-figure)."""
    patches_out = []
    values_out = []
    for feat in geo["features"]:
        props = feat["properties"]
        pc4 = str(props.get("pc4_code") or props.get("pc4") or props.get("postcode", ""))[:4]
        revenue = revenue_by_pc4.get(pc4)
        if not pc4 or revenue is None:
            continue
        geom = feat["geometry"]
        if geom["type"] == "Polygon":
            ring_iter = [geom["coordinates"][0]]
        else:
            ring_iter = [poly[0] for poly in geom["coordinates"]]
        for ring in ring_iter:
            patches_out.append(MplPolygon([(lon, lat) for lon, lat in ring], closed=True))
            values_out.append(revenue)
    return patches_out, values_out


def _pc4_revenue_collection(patches_out: list, values_out: list) -> PatchCollection:
    coll = PatchCollection(patches_out, cmap="YlOrRd", edgecolor="grey", linewidth=0.2)
    coll.set_array(values_out)
    coll.set_norm(Normalize(vmin=min(values_out), vmax=max(values_out)))
    return coll


patches, values = _pc4_revenue_patches(geojson, pc4_revenue)

fig, ax = plt.subplots(figsize=(10, 10))
collection = _pc4_revenue_collection(patches, values)
ax.add_collection(collection)
ax.autoscale()
ax.set_aspect("equal")
ax.set_title("Airbnb avg annual revenue by postcode (PC4)")
ax.set_xlabel("Longitude")
ax.set_ylabel("Latitude")
fig.colorbar(ScalarMappable(norm=collection.norm, cmap=collection.cmap), ax=ax, label="EUR")
plt.tight_layout()
plt.show()

# %% [markdown]
# ## Amsterdam district overlay (L5 — amsterdam_areas.geojson)

# %%
areas_path = REPO_ROOT / "data/input/geo/amsterdam_areas.geojson"
with open(areas_path) as fh:
    areas = json.load(fh)

fig, ax = plt.subplots(figsize=(10, 10))
overlay_patches, overlay_values = _pc4_revenue_patches(geojson, pc4_revenue)
ax.add_collection(_pc4_revenue_collection(overlay_patches, overlay_values))
ax.autoscale()
ax.set_aspect("equal")

for feat in areas.get("features", []):
    name = feat.get("properties", {}).get("name", "")
    geom = feat["geometry"]
    rings = (
        [geom["coordinates"][0]]
        if geom["type"] == "Polygon"
        else [poly[0] for poly in geom["coordinates"]]
    )
    for ring in rings:
        poly = MplPolygon(
            [(lon, lat) for lon, lat in ring],
            closed=True,
            fill=False,
            edgecolor="#333333",
            linewidth=0.8,
            alpha=0.6,
        )
        ax.add_patch(poly)
        if ring:
            cx = sum(c[0] for c in ring) / len(ring)
            cy = sum(c[1] for c in ring) / len(ring)
            ax.text(cx, cy, name, fontsize=6, ha="center")

ax.set_title("Airbnb revenue by PC4 with Amsterdam district boundaries")
plt.tight_layout()
plt.show()

print("Done — charts rendered from", config.gold_comparison_path)
