# Design decisions

Why the pipeline is built this way — not a code walkthrough.

## Problem framing

The assessment asks: *where in Amsterdam should you invest, and via Airbnb or long-term rental?*

That needs:

1. Both datasets on a **common geography** (PC4 postcode)
2. **Comparable revenue** per listing and per area
3. **Transparent assumptions** (occupancy, fees, vacancy)

## Medallion architecture

```
data/input/*.csv|json  →  BRONZE (raw Parquet + metadata)
                        →  SILVER (typed, validated, geo-enriched)
                        →  GOLD (per-listing revenue, PC4 aggregates, comparison)
```

**Why:** Bronze preserves source fidelity for audit/replay. Silver is the single cleaned contract for analytics. Gold holds business logic only — easy to re-run when assumptions change without re-ingesting.

## Missing Airbnb zipcodes (~23%)

**Problem:** 2,254 listings have no zipcode; most have coordinates.

**Options considered:**
- Drop rows → loses coverage, biases toward listed zipcodes
- External API backfill → network dependency, rate limits
- **GeoJSON point-in-polygon** → offline, reproducible, 474 Amsterdam PC4 polygons

**Choice:** Shapely STRtree spatial index on `post_codes.geojson`, applied only to rows missing PC4. `pc4_source` column tracks `raw` vs `geo_enriched`.

Records without coordinates → `_quarantine/`.

## Data quality

| Issue | Approach |
|---|---|
| Mixed zip formats (`1016 AM`, `1055XP`) | Extract first 4 digits as PC4 |
| Bad prices/rents | Validate range in Silver; quarantine failures |
| Outliers (3× IQR) | Flag in Silver; exclude from Gold averages only |
| Non-Amsterdam rows | Filter in Silver (Airbnb by PC4 range, rentals by city) |

**Why quarantine over delete:** Assessment expects handling improper data explicitly; quarantine supports backfill workflows.

## Backfill strategy

When source data improves (e.g. corrected zipcodes):

1. Re-run Bronze (overwrite or partition by `_ingested_at`)
2. Re-run Silver — fewer rows need geo-enrichment
3. Re-run Gold — aggregations pick up changes automatically

No manual intervention in Gold logic.

## Tool choice: local PySpark

**Why not Databricks-only:** Reviewer shouldn't need a cloud account to validate the submission.

**Databricks-ready anyway:**
- `resources/databricks_job.yml` — staged bronze/silver/gold jobs
- `rent_airbnb.jobs` wheel entry points
- `PipelineConfig.from_env()` for path injection
- `dlt/rent_airbnb_pipeline.py` + `databricks.yml` — DLT variant for trial workspace deploy (L3)

Local PySpark remains the primary path for reviewers; DLT mirrors the same quality rules via `@dlt.expect_or_drop`.

## Stretch goals

| Level | Status | Notes |
|---|---|---|
| L1 CI/CD + pre-commit | ✅ | `.github/workflows/ci.yml`, `deploy.yml`, `.pre-commit-config.yaml` |
| L2 Visualisation + diagrams | ✅ | `02_visualisation.py`, `docs/diagrams.md` |
| L3 DLT + expectations + bundle | ✅ | `dlt/rent_airbnb_pipeline.py`, `databricks.yml` |
| L4 Streaming + live gold | ✅ | `streaming.py`, `scripts/prepare_streaming_source.py` |
| L5 Geo-enrich + areas viz | ✅ | `Pc4SpatialIndex`, amsterdam_areas overlay |

Shared quality rules live in `quality.py` and are enforced in Silver (local) and DLT (`@dlt.expect_or_drop`).

## Revenue assumptions

Documented in `schemas.py` so analysts can sensitivity-test:

- Airbnb 65% occupancy, 3% host fee, 365 nights
- Rental 11 occupied months/year (vacancy between tenants)

These are industry-style defaults, not fitted from the dataset — stated explicitly so results are interpretable.
