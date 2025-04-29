CREATE TABLE IF NOT EXISTS raw.load_audit_log (
    id BIGINT AUTOINCREMENT,
    table_name STRING,
    file_path STRING,
    status STRING,
    message STRING,
    start_time TIMESTAMP_LTZ,
    end_time TIMESTAMP_LTZ,
    row_count_loaded NUMBER,
    retry_count NUMBER DEFAULT 0,
    created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);