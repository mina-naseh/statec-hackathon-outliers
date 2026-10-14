CREATE OR REPLACE VIEW comext_monthly AS
SELECT * EXCLUDE (ingestion_source_key)
FROM comext_records;

CREATE OR REPLACE VIEW detailed_lux_imports AS
SELECT *
FROM comext_monthly
WHERE declarant = 'LU' AND flow = 1;

CREATE OR REPLACE VIEW detailed_lux_exports AS
SELECT *
FROM comext_monthly
WHERE declarant = 'LU' AND flow = 2;
