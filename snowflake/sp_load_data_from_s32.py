CREATE OR REPLACE PROCEDURE raw.sp_load_parquet_from_s3_enhanced()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS
$$
from snowflake.snowpark import Session
import datetime

def main(session: Session) -> str:
    tables = session.table("raw.table_list").collect()
    max_retries = 3

    for row in tables:
        table_name = row["table_name"]
        file_path = f"{table_name}.parquet"
        stage_path = f"@odw_stage/{file_path}"
        start_time = datetime.datetime.now()
        retry_count = 0

        while retry_count <= max_retries:
            try:
                # Drop the table if it exists
                session.sql(f"DROP TABLE IF EXISTS raw.{table_name}").collect()

                # Use INFER_SCHEMA in the TEMPLATE clause to create the table
                session.sql(f"""
                    CREATE TABLE raw.{table_name}
                    USING TEMPLATE (
                        SELECT * FROM TABLE(
                            INFER_SCHEMA(
                                LOCATION => '{stage_path}',
                                FILE_FORMAT => 'PARQUET'
                            )
                        )
                    );
                """).collect()

                # Load data into the table
                session.sql(f"""
                    COPY INTO raw.{table_name}
                    FROM {stage_path}
                    FILE_FORMAT = (TYPE = PARQUET)
                    FORCE = TRUE;
                """).collect()

                # Count rows
                row_count = session.table(f"raw.{table_name}").count()

                # Log success
session.sql(f"""
    INSERT INTO raw.load_audit_log (
        table_name, file_path, status, message, start_time, end_time, row_count_loaded, retry_count
    )
    VALUES (
        '{table_name}', '{file_path}', 'SUCCESS', 'Loaded with schema inferred',
        TO_TIMESTAMP_LTZ('{start_time}'), CURRENT_TIMESTAMP(), {row_count}, {retry_count}
    );
""").collect()
                break  # Success

            except Exception as e:
                retry_count += 1
                if retry_count > max_retries:
session.sql(f"""
    INSERT INTO raw.load_audit_log (
        table_name, file_path, status, message, start_time, end_time, row_count_loaded, retry_count
    )
    VALUES (
        '{table_name}', '{file_path}', 'FAILED', $$ {str(e)} $$,
        TO_TIMESTAMP_LTZ('{start_time}'), CURRENT_TIMESTAMP(), NULL, {retry_count}
    );
""").collect()"retry_count": retry_count
                        }
                    ])
    return "All tables processed."
$$;
