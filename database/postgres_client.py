"""Database client."""
import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor


class PostgresClient:
    def __init__(
            self,
            DATABASE_HOST,
            DATABASE_USERNAME,
            DATABASE_PASSWORD,
            DATABASE_PORT,
            DATABASE_NAME,
            DATABASE_SCHEMA
    ):
        self.host = DATABASE_HOST
        self.username = DATABASE_USERNAME
        self.password = DATABASE_PASSWORD
        self.port = DATABASE_PORT
        self.dbname = DATABASE_NAME
        self.schema = DATABASE_SCHEMA
        self.connection = None

    def connect(self):
        """Connect to a Postgres database."""
        if self.connection is None:
            try:
                self.connection = psycopg2.connect(
                    host=self.host,
                    user=self.username,
                    password=self.password,
                    port=self.port,
                    dbname=self.dbname
                )
            except psycopg2.DatabaseError as e:
                raise e

    def select_rows(self, table, select=None, where=None):
        """Run SELECT query and return dictionaries."""
        self.connect()

        clauses = []
        if select:
            if type(select) != list:
                select = [select]
            clauses.append(sql.SQL("SELECT {}").format(
                sql.SQL(', ').join(sql.Identifier(f) for f in select),
            ))
        else:
            clauses.append(sql.SQL("SELECT *"))

        clauses.append(sql.SQL("FROM {}").format(sql.Identifier(self.schema, table)))
        if where and len(where) > 0:
            clauses.append(sql.SQL('WHERE {}').format(
                sql.SQL(' AND ').join(
                    sql.SQL('{} = {}').format(
                        sql.Identifier(f), sql.Literal(v)
                    ) for f, v in where.items()
                )
            ))

        query = sql.Composed(clauses).join(' ')
        with self.connection.cursor(cursor_factory=RealDictCursor) as cur:
            meta, records = {}, []
            cur.execute(query)
            records = cur.fetchall()
        return meta, records

    def insert_rows(self, table, rows, returning=None):
        """Run a SQL query to insert rows in table."""
        self.connect()

        clauses = []
        fields = list(set([key for row in rows for key in row]))
        clauses.append(sql.SQL("INSERT INTO {} ({}) VALUES {}").format(
            sql.Identifier(self.schema, table),
            sql.SQL(', ').join(map(sql.Identifier, fields)),
            sql.SQL(', ').join([
                sql.SQL('({})').format(
                    sql.SQL(', ').join(map(sql.Literal, [row[field] for field in fields]))
                ) for row in rows
            ])
        ))

        if returning == '*':
            clauses.append(sql.SQL('RETURNING *'))
        elif returning:
            if type(returning) != list:
                returning = [returning]
            clauses.append(sql.SQL('RETURNING {}').format(
                sql.SQL(', ').join(sql.Identifier(f) for f in returning),
            ))

        query = sql.Composed(clauses).join(' ')
        with self.connection.cursor(cursor_factory=RealDictCursor) as cur:
            meta, records = {}, []
            cur.execute(query)
            if returning:
                records = cur.fetchall()
            self.connection.commit()
            meta['rows_affected'] = cur.rowcount
        return meta, records

    def update_rows(self, table, changes, where=None, returning=None):
        """Run a SQL query to update rows in table."""
        self.connect()

        clauses = []
        clauses.append(sql.SQL("UPDATE {} SET {}").format(
            sql.Identifier(self.schema, table),
            sql.SQL(', ').join(
                sql.SQL('{} = {}').format(
                    sql.Identifier(f), sql.Literal(v)
                ) for f, v in where.items()
            )
        ))

        if where and len(where) > 0:
            clauses.append(sql.SQL('WHERE {}').format(
                sql.SQL(' AND ').join(
                    sql.SQL('{} = {}').format(
                        sql.Identifier(f), sql.Literal(v)
                    ) for f, v in where.items()
                )
            ))

        if returning == '*':
            clauses.append(sql.SQL('RETURNING *'))
        elif returning:
            if type(returning) != list:
                returning = [returning]
            clauses.append(sql.SQL('RETURNING {}').format(
                sql.SQL(', ').join(sql.Identifier(f) for f in returning),
            ))

        query = sql.Composed(clauses).join(' ')
        with self.connection.cursor(cursor_factory=RealDictCursor) as cur:
            meta, records = {}, []
            cur.execute(query)
            if returning:
                records = cur.fetchall()
            self.connection.commit()
            meta['rows_affected'] = cur.rowcount
        return meta, records
