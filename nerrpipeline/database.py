from typing import Optional
import psycopg2

from logger import AppLogger
from credentials import CREDENTIALS


applogger = AppLogger.default_logger(__name__)

"""
    Store DB Connection objects...
"""


class DatabaseConnection:
    """
        Base class used to create DB connections...
    """

    def __init__(self, credentials: CREDENTIALS) -> None:
        self.credentials = credentials

    def create_connection(self):
        applogger.info("Creating DB connection...")

        creds = self.credentials.get_credentials_from_json()
        host = creds.DB_HOST
        database = creds.DB_NAME
        user = creds.DB_USER
        password = creds.DB_PASSWORD
        port = creds.DB_PORT

        conn = DatabaseConnection.create_db_connection(
            host,
            database,
            user,
            password,
            port
        )
        return conn

    # Sigh, beam cannot pickle the Connection type hint...
    # This method returns a connection object
    @staticmethod
    def create_db_connection(
        host: str,
        database: Optional[str],
        user: Optional[str],
        password: Optional[str],
        port: str = None,
        timeout: int = 60
    ):

        """
            Method used to create the psycopg2 connection
        """

        try:
            connection = psycopg2.connect(
                host=host,
                dbname=database,
                user=user,
                password=password,
                port=port,
                connect_timeout=timeout
            )
        except Exception as err:
            applogger.exception(err, exc_info=True)
            raise Exception(err)
        return connection


class EnvironmentalDataConnection(DatabaseConnection):
    """
        Class with default credentials object to connect to the EV DB.
    """
    def __init__(self, credentials: CREDENTIALS = CREDENTIALS) -> None:
        super().__init__(credentials)
