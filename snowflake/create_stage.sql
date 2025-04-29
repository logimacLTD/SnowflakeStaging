CREATE OR REPLACE STAGE odw_stage
    STORAGE_INTEGRATION = s3_integration
    URL = 's3://transformed/odw/'
    FILE_FORMAT = (TYPE = PARQUET);