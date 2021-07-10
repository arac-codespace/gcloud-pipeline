import unittest
from postgresql_sink import WriteToPostgres
import apache_beam as beam
from tests.input_data import NUTRIENTS_PARSED_REAL_ROW
from database import EnvironmentalDataConnection
from datasource import NutrientsSchema


class Test_Postgres_Sink(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print("setupClass")

    def setUp(self):
        records = [
            NUTRIENTS_PARSED_REAL_ROW,
            NUTRIENTS_PARSED_REAL_ROW
        ]
        self.records = records
        self.table = '"NERRNutrients"'
        self.temp_table = "nerr_test_table"
        self.db_conn_obj = EnvironmentalDataConnection()
        self.schema = NutrientsSchema
        self.columns = self.schema._fields

    def test_process(self):

        conn = self.db_conn_obj.create_connection()
        cursor = conn.cursor()

        # Drop Temporary Table if exists
        sql = f"DROP Table IF EXISTS {self.temp_table}"
        cursor.execute(sql)
        conn.commit()

        # Create test table..
        sql = f'CREATE TABLE {self.temp_table} AS TABLE {self.table} WITH NO DATA'
        cursor.execute(sql)
        conn.commit()

        # Insert rows using beam pipeline...
        with beam.Pipeline() as p:
            output = (
                p
                | "GetTestData" >> beam.Create(self.records)
                | "Map To Schema" >> beam.Map(lambda row: self.schema(**row)).with_output_types(self.schema)
                | "WriteData" >> WriteToPostgres(self.db_conn_obj, self.temp_table, self.columns)
            )

            print(output)

        # Check insert was succesful...
        sql = f"SELECT * FROM {self.temp_table}"
        cursor.execute(sql)
        rows = cursor.fetchall()
        rows = [row for row in rows]

        self.assertEqual(len(self.records), len(rows))

        # Drop Temporary Table
        sql = f"DROP Table IF EXISTS {self.temp_table}"
        cursor.execute(sql)
        conn.commit()
        conn.close()
