def test_failure_callback_import():
    from airflow_plugins.failure_alerts import failure_callback
    assert callable(failure_callback)