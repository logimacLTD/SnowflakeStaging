import snowflake.connector

def test_snowflake_connection():
    conn = snowflake.connector.connect(
        user="your_user",
        password="your_password",
        account="your_account",
        warehouse="your_warehouse",
        database="your_database",
        schema="your_schema",
        role="your_role"
    )
    cursor = conn.cursor()
    cursor.execute("SELECT CURRENT_TIMESTAMP()")
    result = cursor.fetchone()
    assert result is not None
    cursor.close()
    conn.close()