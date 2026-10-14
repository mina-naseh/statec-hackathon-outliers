# Database schema

This project commits detailed observations and ingestion checkpoints to DuckDB in batches.
Monthly Parquet files are exports of completed source files.

## Fact table

`comext_records` is the physical fact table. `comext_monthly` is its public view, excluding
the internal `ingestion_source_key`. Rows become visible after each batch commit.

The schema is intentionally lossless for the source rows that are retained after Luxembourg-only filtering:

- period: INTEGER
- declarant: VARCHAR
- partner: VARCHAR
- product_nc: VARCHAR
- flow: INTEGER
- value_in_euros: BIGINT
- quantity_in_kg: BIGINT
- sup_quantity: BIGINT
- source_archive: VARCHAR
- source_row_number: BIGINT (one-based logical data record number, excluding the header)
- classification_version: VARCHAR
- extra_source_columns: JSON (original source fields when preservation is enabled)
- ingestion_source_key: VARCHAR (physical table only; SHA-256 of the absolute source path)

The physical primary key is `(ingestion_source_key, source_row_number)`.
Initialization preserves saved records and migrates an existing Parquet-backed view once.

The monthly partitions are written under paths resembling:

- `data/processed/year=2025/month=01/source-<source-key>.parquet`

An export failure does not roll back committed database rows. On restart, an EOF checkpoint
retries its exports without reparsing. Temporary export files do not end in `.parquet`.

## Checkpoints

`ingestion_checkpoint` tracks each source path, source key, file size and modification time,
parser settings, byte offset, scanned and retained row counts, rejection counts,
EOF flag, status, last error, exported file paths, and update time.

Observations and their checkpoint commit in one transaction. Status is `parsing`,
`exporting`, `completed`, or `failed`; a hard stop can leave `parsing` or `exporting`,
which is also resumable. Completed sources are skipped on reruns. Changed source files
or parser settings are rejected instead of silently appending a different revision.

`ingestion_log` records run attempts (`running`, `completed`, `failed`, `interrupted`).
The next run marks a previous abruptly stopped run as interrupted. An OS file lock prevents
two ingest processes from using the same database and is released after a process exits.

## Metadata tables

- `partner_dim`: official partner codelist table
- `product_dim`: official product codelist table
- `product_correspondence`: official classification relationships
- `source_archive`: provenance and download metadata
- `ingestion_log`: ingestion attempts and outcomes

## Notes

- The view reads committed DuckDB records. Parquet exports can be queried separately
  using `read_parquet('data/processed/**/source-*.parquet', union_by_name=true)`.
  Legacy `part.parquet` exports remain on disk and are excluded from this pattern.
- Writer connections close between batches. Short external queries can run while parsing
  or extracting; persistent external connections can block subsequent writes.
- No PostgreSQL-specific partitioning syntax is used.
- Aggregate and source-reported rows are retained in the raw dataset but should be filtered out of default product-level summaries when not appropriate.
