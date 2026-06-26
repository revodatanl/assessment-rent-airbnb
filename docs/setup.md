# Setup & run

## Prerequisites

| Tool | Version |
|---|---|
| Python | 3.10+ |
| Java | 11 or 17 (required by Spark) |

macOS: `brew install openjdk@17`  
Ubuntu: `sudo apt install openjdk-17-jdk`

## 1. Get the data

From repo root, unzip archives in `data/input/`:

```bash
unzip -o data/input/airbnb.zip -d data/input
unzip -o data/input/rentals.zip -d data/input
unzip -o data/input/geo/post_codes.zip -d data/input/geo/
unzip -o data/input/geo/amsterdam_areas.zip -d data/input/geo/
```

Verify:

```bash
ls data/input/airbnb.csv data/input/rentals.json
ls data/input/geo/post_codes.geojson data/input/geo/amsterdam_areas.geojson
```

## 2. Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

## 3. Run pipeline

Run from **repo root** (paths are relative to cwd):

```bash
export PYSPARK_PYTHON=.venv/bin/python
export PYSPARK_DRIVER_PYTHON=.venv/bin/python
python -m rent_airbnb.pipeline
```

Or: `make run`

Pipeline prints **top investment opportunities** at the end. Parquet lands in `data/output/`.

Runtime ~30s–2min locally (geo-enrichment is the slow step).

## 4. Visualise

After the pipeline completes:

```bash
# From repo root — run pipeline first (step 3)
python notebooks/02_visualisation.py
```

Requires `matplotlib` and `pandas` (included in `[dev]`). Notebook resolves paths from repo root automatically.

## 5. Tests & lint

```bash
make test
make lint
pre-commit install   # optional
pre-commit run --all-files
```

Expected: **50 passed**.

## Environment variables

| Variable | Default |
|---|---|
| `AIRBNB_CSV_PATH` | `data/input/airbnb.csv` |
| `RENTALS_JSON_PATH` | `data/input/rentals.json` |
| `POST_CODES_GEOJSON_PATH` | `data/input/geo/post_codes.geojson` |
| `OUTPUT_BASE` | `data/output` |
| `GEO_ENRICH` | `true` |
| `PIPELINE_LOG_COUNTS` | `false` (set `true` for row-count logs) |
| `OUTPUT_COALESCE_PARTITIONS` | unset (set `1` for single-file local Parquet) |

## Troubleshooting

**`BindException: sparkDriver`** — macOS/VPN hostname issue. Pipeline sets `127.0.0.1` for local runs; ensure no stale Spark processes.

**`No module named pyspark'`** — activate `.venv`.

**`FileNotFoundError: data/input/airbnb.csv`** — unzip Step 1 or run from repo root.

**Python worker version mismatch** — set `PYSPARK_PYTHON` and `PYSPARK_DRIVER_PYTHON` to your venv python.

**Slow geo step (~60–90s)** — normal for ~2,250 point-in-polygon lookups; Silver is cached to Parquet afterward.
