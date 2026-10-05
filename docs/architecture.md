# Architecture

## Pipeline

```mermaid
flowchart LR
  V["Vendor JSON drops<br/>3 vendors + orders export"] --> M[("S3 bucket<br/>(MinIO locally)")]
  M --> D["Airflow DAG<br/>discover, hash, validate, load"]
  D -->|"bad file"| Q[("quarantine/<br/>file + reason")]
  D -->|"good file"| R[("Postgres raw.records<br/>+ ops.file_manifest")]
  R --> T["dbt: staging,<br/>intermediate, marts"]
  T --> X["SKU crosswalk<br/>to order lines"]
  T --> H["Pipeline health<br/>report"]
```

## What happens to one file

```mermaid
flowchart TD
  A[File lands in incoming/feed/date.json] --> B[Read bytes, sha256]
  B --> C{Checksum already in manifest?}
  C -->|yes| S[Skip. A re-run loads nothing]
  C -->|no| E{Valid JSON and matches the feed contract?}
  E -->|no| Q["Copy to quarantine/, write reason file,<br/>manifest row 'quarantined', alert.<br/>Other files keep going"]
  E -->|yes| L["One transaction: manifest row 'loaded'<br/>+ every record into raw.records"]
```

## Airflow

```mermaid
flowchart LR
  subgraph ingest_vendor_files
    I["ingest (one mapped task per feed)"] --> F[check_freshness]
    I --> P["publish_raw_records<br/>(updates the raw records asset)"]
  end
  P -.->|asset update| TR
  subgraph transform
    TR[dbt_build] --> RR[render_report]
  end
```

## Warehouse layers

| Schema | Owner | Contents |
|---|---|---|
| `raw` | ingest DAG | `records`: one JSONB document per record, exactly as received |
| `ops` | ingest DAG | `file_manifest`: one row per distinct file content, loaded or quarantined, with the reason |
| `staging` | dbt | typed views per source; the newest loaded version of each day wins |
| `intermediate` | dbt | normalized order lines, latest vendor product snapshots |
| `marts` | dbt | `dim_products`, `crosswalk_sku`, `fct_order_lines`, `fct_orders`, `sku_match_summary` |
