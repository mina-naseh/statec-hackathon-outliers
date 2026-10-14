CREATE TABLE IF NOT EXISTS source_archive (
    source_archive_id INTEGER,
    source_path VARCHAR,
    source_url VARCHAR,
    filename VARCHAR,
    reference_period VARCHAR,
    file_size BIGINT,
    source_modified_at TIMESTAMP,
    checksum VARCHAR,
    local_path VARCHAR,
    downloaded_at TIMESTAMP,
    parsed_record_count BIGINT,
    imported_record_count BIGINT
);

CREATE TABLE IF NOT EXISTS ingestion_log (
    run_id VARCHAR,
    source_name VARCHAR,
    status VARCHAR,
    error_message VARCHAR,
    processed_periods VARCHAR,
    started_at TIMESTAMP,
    finished_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS partner_dim (
    partner_code VARCHAR,
    label_en VARCHAR,
    label_fr VARCHAR,
    label_de VARCHAR,
    is_standard_code BOOLEAN,
    source VARCHAR,
    note VARCHAR,
    ec_corporate_code VARCHAR,
    ec_corporate_uri VARCHAR,
    codelist_version VARCHAR
);

CREATE TABLE IF NOT EXISTS product_dim (
    product_code VARCHAR,
    label_en VARCHAR,
    label_fr VARCHAR,
    label_de VARCHAR,
    level INTEGER,
    parent_code VARCHAR,
    chapter VARCHAR,
    is_not_elsewhere_specified BOOLEAN,
    classification_id VARCHAR,
    classification_version VARCHAR
);

CREATE TABLE IF NOT EXISTS product_correspondence (
    source_classification VARCHAR,
    source_version VARCHAR,
    source_product_code VARCHAR,
    target_classification VARCHAR,
    target_version VARCHAR,
    target_product_code VARCHAR,
    relationship_type VARCHAR,
    source_metadata_file VARCHAR
);

CREATE TABLE IF NOT EXISTS comext_records (
    period INTEGER,
    declarant VARCHAR,
    partner VARCHAR,
    product_nc VARCHAR,
    flow INTEGER,
    value_in_euros BIGINT,
    quantity_in_kg BIGINT,
    sup_quantity BIGINT,
    source_archive VARCHAR,
    source_row_number BIGINT,
    classification_version VARCHAR,
    extra_source_columns JSON,
    ingestion_source_key VARCHAR,
    PRIMARY KEY (ingestion_source_key, source_row_number)
);

CREATE TABLE IF NOT EXISTS ingestion_checkpoint (
    source_key VARCHAR PRIMARY KEY,
    source_path VARCHAR UNIQUE,
    file_size BIGINT,
    modified_ns BIGINT,
    parser_signature VARCHAR,
    byte_offset BIGINT DEFAULT 0,
    parsed_rows BIGINT DEFAULT 0,
    retained_rows BIGINT DEFAULT 0,
    missing_declarant BIGINT DEFAULT 0,
    non_luxembourg BIGINT DEFAULT 0,
    invalid_flow BIGINT DEFAULT 0,
    invalid_period BIGINT DEFAULT 0,
    at_eof BOOLEAN DEFAULT FALSE,
    status VARCHAR DEFAULT 'parsing',
    error_message VARCHAR,
    parquet_files JSON DEFAULT '[]',
    updated_at TIMESTAMP DEFAULT current_timestamp
);
