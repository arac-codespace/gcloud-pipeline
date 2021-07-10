from database import DatabaseConnection
import apache_beam as beam
from typing import List, NamedTuple
from psycopg2.extras import RealDictCursor, execute_values
from logger import AppLogger

applogger = AppLogger.default_logger(__name__)


# Based on Mitchell's solution https://github.com/mitchelllisle/beam-sink
class WriteToPostgres(beam.PTransform):
    """
        Insert Data into Postgres DB
    """
    def __init__(
        self,
        db_conn_obj: DatabaseConnection,
        table: str,
        columns: List[str],
        **kwargs
    ):
        super().__init__()
        self.db_conn_obj = db_conn_obj
        self.table = table
        self.columns = columns
        self.kwargs = kwargs

    def expand(self, pcoll):
        return (
            pcoll
            | beam.BatchElements(**self.kwargs)
            | beam.ParDo(_Insert(self.db_conn_obj, self.table, self.columns))
        )


class _PutFn(beam.DoFn):
    """
        A DoFn that takes care of making the connection to the
        database and closing it.

        start_bundle and finish_bundle are the methods that it
        will call before calling the process method.
    """
    def __init__(self, db_conn_obj: DatabaseConnection, table: str, columns: List[str]):
        super().__init__()
        self.db_conn_obj = db_conn_obj
        self.table = table
        self.columns = columns
        self.conn = None
        self.cursor = None

    def start_bundle(self) -> None:
        self.conn = self.db_conn_obj.create_connection()
        self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)

    def process(self, element) -> None:
        raise NotImplementedError

    def finish_bundle(self):
        self.conn.close()


class _Insert(_PutFn):
    """
        Inherits from _PutFn, and it's used to implement the
        process method.

        Expects a schema which is nothing more than
    """
    def process(self, records: List[NamedTuple]) -> None:
        applogger.info(f"Inserting {len(records)} rows to {self.table}")

        if records:
            table = self.table
            columns = self.columns
            # Wrap columns in double quotes in case columns are capitalized
            columns_str = ",".join([f'"{col}"' for col in columns])

            values = [list(value for value in record) for record in records]

            sql = f"INSERT INTO {table} ({columns_str}) VALUES %s"

            execute_values(
                self.cursor,
                sql,
                values
            )

            self.conn.commit()
