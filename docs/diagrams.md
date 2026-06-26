# Architecture & CI diagrams (L2)

## Medallion data flow

```mermaid
flowchart LR
    subgraph inputs [Inputs]
        A[airbnb.csv]
        R[rentals.json]
        G[post_codes.geojson]
    end

    subgraph bronze [Bronze]
        BA[bronze/airbnb]
        BR[bronze/rentals]
    end

    subgraph silver [Silver]
        SA[silver/airbnb]
        SR[silver/rentals]
        Q[_quarantine]
    end

    subgraph gold [Gold]
        GLA[airbnb_listing_revenue]
        GLR[rentals_listing_revenue]
        GA[airbnb_by_pc4]
        GR[rentals_by_pc4]
        GC[investment_comparison]
        GT[top_opportunities]
    end

    A --> BA
    R --> BR
    BA --> SA
    BR --> SR
    G -.geo-enrich.-> SA
    SA --> GLA
    SR --> GLR
    SA --> GA
    SR --> GR
    GA --> GC
    GR --> GC
    GC --> GT
    SA -.invalid.-> Q
    SR -.invalid.-> Q
```

## CI/CD pipeline (L1)

```mermaid
flowchart LR
    push[git push / PR] --> ci[GitHub Actions CI]
    ci --> lint[ruff lint + format]
    ci --> test[pytest + PySpark]
    manual[workflow_dispatch] --> deploy[Databricks Deploy workflow]
    deploy --> wheel[pip wheel]
    wheel --> bundle[databricks bundle deploy]
    bundle --> jobs[Batch Job]
    bundle --> dlt[DLT Pipeline]
    bundle --> stream[Streaming Job]
```

## Streaming path (L4)

```mermaid
sequenceDiagram
    participant Src as rentals/*.json
    participant SS as Structured Streaming
    participant Br as Bronze Parquet
    participant Si as Silver Parquet
    participant Go as Gold tables

    Src->>SS: maxFilesPerTrigger=1
    SS->>Br: append batch
    SS->>Si: rebuild rentals silver
    SS->>Go: refresh comparison + top N
```

## DLT expectations (L3)

Quality rules on `silver_airbnb` and `silver_rentals` mirror `src/rent_airbnb/quality.py`:

| Table | Expectation | Action |
|---|---|---|
| silver_airbnb | valid_pc4 | drop |
| silver_airbnb | valid_price (10–5000) | drop |
| silver_airbnb | valid_room_type | drop |
| silver_rentals | valid_rent (200–4000) | drop |
| silver_rentals | valid_pc4 | drop |
| silver_rentals | valid_property_type | drop |

DLT definition: `dlt/rent_airbnb_pipeline.py`
