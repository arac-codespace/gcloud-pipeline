from credentials import EVCREDENTIALS
import unittest
from database import DatabaseConnection
from psycopg2 import extensions
from logger import AppLogger

applogger = AppLogger.default_logger(__name__)


class Test_Database_Conn(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        applogger.info("Setting up Database Connection Test...")

    def setUp(self):
        creds = EVCREDENTIALS()
        self.host = creds.DB_HOST
        self.database = creds.DB_NAME
        self.user = creds.DB_USER
        self.password = creds.DB_PASSWORD
        self.port = creds.DB_PORT

    def test_environmental_data_connection(self):
        applogger.info("Testing connection to the environmental data DB...")

        conn = DatabaseConnection.create_db_connection(
            self.host,
            self.database,
            self.user,
            self.password,
            self.port
        )

        conn_status = conn.status
        expected_statuses = [
            extensions.STATUS_READY,
            extensions.STATUS_BEGIN
        ]
        is_ok = conn_status in expected_statuses

        self.assertTrue(is_ok)

        conn.close()
