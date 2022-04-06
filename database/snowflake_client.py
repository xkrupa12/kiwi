from snowflake import connector as snowflake_connector


class Snowflake:
    cursor = None

    def __init__(self, user: str, password: str, account: str, role: str):
        self.connector = snowflake_connector.connect(
            user=user,
            password=password,
            account=account,
            role=role
        )

    def __del__(self):
        if self.cursor:
            self.cursor.close()

    def get_cursor(self):
        if not self.cursor:
            self.cursor = self.connector.cursor()

        return self.cursor

    def test_connection(self):
        cursor = self.connector.cursor()
        try:
            cursor.execute("SELECT current_version()")
            one_row = cursor.fetchone()
            print(one_row[0])
        finally:
            cursor.close()
