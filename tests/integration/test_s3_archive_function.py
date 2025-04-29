import boto3
from moto import mock_s3

@mock_s3
def test_archive_loaded_files():
    import dags.odw_snowflake_loader as loader

    s3 = boto3.client('s3', region_name='us-east-1')
    s3.create_bucket(Bucket='transformed')
    s3.put_object(Bucket='transformed', Key='odw/test.parquet', Body=b'test')

    loader.archive_loaded_files()

    objs = s3.list_objects_v2(Bucket='transformed', Prefix='odw/archive/')
    assert 'Contents' in objs