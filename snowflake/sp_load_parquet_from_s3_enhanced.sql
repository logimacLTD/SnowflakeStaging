CREATE OR REPLACE PROCEDURE raw.sp_load_parquet_from_s3_enhanced()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_file STRING;
    v_table STRING;
    v_start_time TIMESTAMP_LTZ;
    v_end_time TIMESTAMP_LTZ;
    v_row_count NUMBER;
    v_sql TEXT;
    v_retry_count NUMBER DEFAULT 0;
    v_max_retries NUMBER DEFAULT 3;
BEGIN
    FOR record IN 
        SELECT METADATA$FILENAME AS file_path
        FROM @odw_stage
        WHERE METADATA$FILENAME NOT LIKE '%/archive/%'
    DO
        BEGIN
            LET v_start_time = CURRENT_TIMESTAMP();
            LET v_file = record.file_path;
            LET v_table = SPLIT_PART(SPLIT_PART(v_file, '/', -1), '.', 1); -- Extract file name without extension

            -- Create table if not exists
            EXECUTE IMMEDIATE '
                CREATE TABLE IF NOT EXISTS raw.' || v_table || ' USING TEMPLATE (
                    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) FROM @odw_stage/' || v_file || '
                );
            ';

            -- Load data using optimized COPY INTO
            EXECUTE IMMEDIATE '
                COPY INTO raw.' || v_table || '
                FROM @odw_stage/' || v_file || '
                FILE_FORMAT = (TYPE = PARQUET)
                FORCE = TRUE;
            ';

            LET v_row_count = (SELECT COUNT(*) FROM raw. :v_table);

            INSERT INTO raw.load_audit_log (table_name, file_path, status, message, start_time, end_time, row_count_loaded)
            VALUES (:v_table, :v_file, 'SUCCESS', 'Loaded successfully', v_start_time, CURRENT_TIMESTAMP(), v_row_count);

        EXCEPTION
            WHEN OTHER THEN
                LET v_retry_count = v_retry_count + 1;
                IF v_retry_count <= v_max_retries THEN
                    CONTINUE;
                ELSE
                    INSERT INTO raw.load_audit_log (table_name, file_path, status, message, start_time, end_time, retry_count)
                    VALUES (:v_table, :v_file, 'FAILED', ERROR_MESSAGE(), v_start_time, CURRENT_TIMESTAMP(), v_retry_count);
                END IF;
        END;
    END FOR;
    RETURN 'All files processed.';
END;
$$;