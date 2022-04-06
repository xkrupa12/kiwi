from database.postgres_client import PostgresClient
from database.snowflake_client import Snowflake


def connect_to_snowflake() -> None:
    SNOWFLAKE_USER = ''
    SNOWFLAKE_PASSWORD = ''
    SNOWFLAKE_ACCOUNT = ''
    SNOWFLAKE_ROLE = ''

    snowflake = Snowflake(SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT, SNOWFLAKE_ROLE)

    cursor = snowflake.get_cursor()
    result = cursor.execute('SELECT 1')
    print(result.fetchall())


def connect_to_postgres():
    DATABASE_HOST = 'localhost'
    DATABASE_PORT = 5432
    DATABASE_USERNAME = 'postgres'
    DATABASE_PASSWORD = 'pass'
    DATABASE_NAME = 'kiwi'
    DATABASE_SCHEMA = 'public'

    postgres = PostgresClient(DATABASE_HOST, DATABASE_USERNAME, DATABASE_PASSWORD, DATABASE_PORT, DATABASE_NAME, DATABASE_SCHEMA)

    meta, data = postgres.select_rows('sample')
    print(data)

connect_to_postgres()
