# Amsterdam Property Investment Pipeline

PySpark medallion pipeline that compares **Airbnb short-term** vs **Kamernet long-term** rental revenue per Amsterdam postcode (PC4), to identify where investment is more profitable.

Built for the [RevoData technical assessment](https://github.com/revodatanl/assessment-rent-airbnb).

## What it answers

> For each Amsterdam PC4, should I rent via Airbnb or Kamernet?

Gold output ranks postcodes by revenue potential and gives a `recommendation` per area (`airbnb`, `rental`, `neutral`, etc.).

## Why these design choices

| Decision | Why |
|---|---|
| **Medallion (Bronze → Silver → Gold)** | Raw audit trail, reusable cleaned layer, analytics isolated in Gold |
| **Local PySpark** | Reviewer can run without Databricks; job YAML + wheel entry points for cloud deploy |
| **Geo-enrich missing zipcodes** | 22.7% of Airbnb rows lack PC4 but have lat/lon — dropping them biases results |
| **Quarantine, not silent drop** | Bad rows kept in `_quarantine/` for inspection and reprocessing |
| **Flag outliers, exclude from averages** | €9k/night penthouses are real but shouldn't skew PC4 means |
| **Explicit revenue constants** | Occupancy, fees, months/year in `schemas.py` — assumptions visible and overridable |

See [docs/decisions.md](docs/decisions.md) for full rationale.

## Quick start

**Prerequisites:** Python 3.10+, Java 11/17

```bash
# 1. Unzip data (from repo root)
unzip data/input/airbnb.zip -d data/input
unzip data/input/rentals.zip -d data/input
unzip data/input/geo/post_codes.zip -d data/input/geo/
unzip data/input/geo/amsterdam_areas.zip -d data/input/geo/

# 2. Install
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

# 3. Run pipeline
export PYSPARK_PYTHON=.venv/bin/python
export PYSPARK_DRIVER_PYTHON=.venv/bin/python
python -m rent_airbnb.pipeline
```

Or: `make install && make run`

Full setup: [docs/setup.md](docs/setup.md)

## Output

Parquet under `data/output/`:

| Layer | Path | Content |
|---|---|---|
| Bronze | `bronze/airbnb/`, `bronze/rentals/` | Raw ingested data + lineage metadata |
| Silver | `silver/airbnb/`, `silver/rentals/` | Cleaned, typed, geo-enriched listings |
| Silver | `silver/*/_quarantine/` | Rejected records |
| Gold | `gold/airbnb_listing_revenue/`, `gold/rentals_listing_revenue/` | Per-listing annual revenue |
| Gold | `gold/airbnb_by_pc4/`, `gold/rentals_by_pc4/` | PC4-level aggregates |
| Gold | `gold/investment_comparison/` | Side-by-side PC4 comparison |
| Gold | `gold/top_opportunities/` | Top 20 PC4s by Airbnb revenue |

The CLI prints the top-opportunities table at the end.

## Project layout

```
src/rent_airbnb/     Pipeline package (bronze, silver, gold, jobs)
tests/               Unit tests (50)
notebooks/           Pipeline walkthrough + visualisation
scratch/             EDA / validation checks
resources/           Databricks job + DLT + streaming YAML
dlt/                 Delta Live Tables pipeline (L3)
databricks.yml       Asset bundle root
docs/                Setup guide + design decisions
data/input/          Source datasets (zips + extracted files)
data/output/         Generated Parquet (gitignored)
```

## Development

```bash
make test          # pytest
make lint          # ruff check + format
pre-commit install # optional — runs ruff on commit
```

CI: `.github/workflows/ci.yml` runs lint + tests on push.

## Databricks

Wheel entry points: `bronze_job`, `silver_job`, `gold_job`, `pipeline_job`, `streaming_job`.

```bash
pip wheel . -w dist --no-deps
databricks bundle deploy -t dev    # requires DATABRICKS_HOST + DATABRICKS_TOKEN
```

Resources: batch job, DLT pipeline, streaming job — see `databricks.yml`.

## Stretch goals implemented

| Level | Item | Location |
|---|---|---|
| **L1** | Pre-commit + CI + deploy | `.pre-commit-config.yaml`, `.github/workflows/` (`deploy.yml` is manual `workflow_dispatch`) |
| **L2** | Visualisation + diagrams | `notebooks/02_visualisation.py`, `docs/diagrams.md` |
| **L3** | DLT + expectations + bundle | `dlt/rent_airbnb_pipeline.py`, `resources/dlt_pipeline.yml` |
| **L4** | Streaming + live gold | `streaming.py`, `make stream` |
| **L5** | Geo-enrich + areas map | `Pc4SpatialIndex`, amsterdam_areas overlay |

```bash
make run && make stream-prepare && make stream   # L4 demo
```

## Revenue model

**Airbnb:** `price × 0.65 occupancy × 365 nights × 0.97 (after 3% fee)`

**Kamernet:** `(rent + additional_costs) × 11 months`

Constants in `src/rent_airbnb/schemas.py`.
