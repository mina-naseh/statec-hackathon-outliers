# lux_comext

This package downloads, extracts, normalizes, and ingests Luxembourg Comext trade files into a local DuckDB + Parquet-based offline store.

## Prerequisites

- Python 3.12+
- Internet access for the Eurostat API
- A local project checkout with the configured settings file at `config/settings.yaml`

## Setup

From the repository root:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e .
```

If you want the development tools as well:

```powershell
python -m pip install -e ".[dev]"
```

## Discover available files

This reads the configured download sources and lists the matching remote files from the Eurostat files API:

```powershell
python -m lux_comext discover
```

Examples of the output include the source name, file name, period, and size.

## Preview downloads

To refresh the Eurostat listing and validate every planned download URL without
downloading archives or creating local files:

```powershell
python -m lux_comext download --dry-run
```

Each run uses the links published in the configured listing (by default,
<https://ec.europa.eu/eurostat/api/dissemination/files/?sort=1&dir=comext%2FCOMEXT_DATA%2FPRODUCTS>).
It checks each URL with HEAD, falling back to a streamed GET when HEAD is unsupported.
Listing failures, HTTP errors, and HTML/JSON error pages fail the command with a nonzero exit code.

## Download the configured data

To actually fetch the configured Comext archives and metadata files into the local raw data area:

```powershell
python -m lux_comext download
```

Files are stored under the project raw-data directories using the configured storage paths in `config/settings.yaml`.

## Import official metadata

Download and import the official partner and product codelists used for dimension metadata:

```powershell
python -m lux_comext import-metadata
```

## Ingest the downloaded observations

After the archive files are downloaded locally, run the Luxembourg-specific ingestion step:

```powershell
python -m lux_comext ingest
```

This filters source rows to Luxembourg observations and commits them to DuckDB as it parses.
`comext_monthly` exposes saved rows immediately after each batch. The batch limit is
`processing.batch_size` (200,000 scanned rows by default); `processing.commit_interval_seconds`
also triggers a commit after 10 seconds of parsing. Memory use is bounded by one batch.

Each commit saves the observations and CSV byte offset in the same transaction.
If you interrupt the command or it fails, run the same command again: completed files are
skipped and partial files resume from their last committed offset. Only the unfinished batch
is read again. CSV records with quoted multiline values are supported.

Archives are processed one at a time. Complete existing extractions are reused after checking
member file sizes; interrupted extractions are retried. When a source file reaches its end,
its observations are exported to monthly Parquet files under the processed-data directory.
Exports use temporary files and can be retried without duplicating database rows.

Inspect saved data and progress with:

```sql
SELECT COUNT(*) FROM comext_monthly;
SELECT source_path, status, parsed_rows, retained_rows, byte_offset, error_message
FROM ingestion_checkpoint;
```

`python -m lux_comext status` reports saved rows and progress by status. DuckDB connections
are closed between writes, so short queries from another process can run during parsing or
extraction. DuckDB still locks its database during writes; close inspection connections
promptly so they do not block the next commit. A second ingest writer is rejected.

The source size, modification time, and parser settings must match the checkpoint.
Changed sources are rejected to avoid mixing revisions. Re-ingest revised datasets into
a separate database. Existing rows from the previous Parquet-backed view are migrated
once into DuckDB; initialization no longer clears the view.

## Run the full pipeline

For a complete one-time workflow (discovery, download, metadata import, and ingestion):

```powershell
python -m lux_comext run
```

## Validate the local database

Check that the DuckDB database exists and is ready to query:

```powershell
python -m lux_comext validate
```

## Inspect the database status

```powershell
python -m lux_comext status
```

## Typical workflow

```powershell
python -m lux_comext discover
python -m lux_comext download --dry-run
python -m lux_comext download
python -m lux_comext import-metadata
python -m lux_comext ingest
python -m lux_comext validate
python -m lux_comext status
```

## Notes

- CLI commands log to stderr and `logs/pipeline.log` with timestamps, levels, and module names.
  The default level is `DEBUG`; change `logging.level` in `config/settings.yaml` to adjust it.
  Download logs include listing requests, per-file validation progress, HTTP status codes,
  skipped local files, downloaded byte counts, and step durations. Failures include tracebacks.
  Ingest logs include each archive and data file, an extraction heartbeat every 10 seconds,
  parsing progress every 100,000 rows or 10 seconds (including filtered rows), partition writes,
  committed batch counts and offsets, resumed/skipped files, metadata totals, and elapsed time.
  Restart a running command to pick up code changes.
- The package uses the downloadable inventory as the source of truth; it does not request wildcard filenames directly.
- The raw archive files are preserved unchanged.
- DuckDB is the durable ingestion store; Parquet partitions are exports for offline queries.
