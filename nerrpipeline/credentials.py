from __future__ import annotations
import os
from typing import Optional
from psycopg2.extensions import connection
from app_constants import DEFAULT_CREDENTIAL_FILE
import json

"""
    Store code that creates DB Connection objects
    using psycopg2.
"""

def get_creds_from_json(file):
    with open(file) as f:
        creds = json.load(f)
    return creds

class CREDENTIALS:
    """
    Class creates credentials objects
    """

    def __init__(
        self, DB_HOST: str, DB_NAME: str, DB_USER: str, DB_PASSWORD: str, DB_PORT: str
    ) -> None:

        self.DB_HOST = DB_HOST
        self.DB_NAME = DB_NAME
        self.DB_USER = DB_USER
        self.DB_PASSWORD = DB_PASSWORD
        self.DB_PORT = DB_PORT

    @classmethod
    def get_credentials_from_json(cls, file = DEFAULT_CREDENTIAL_FILE) -> CREDENTIALS:
        """
        Creates a credential object holding
        information to connect to the environmental
        database.
        """
        creds = get_creds_from_json(file)

        DB_HOST = creds.get("cloudsql").get("DB_HOST")
        DB_NAME = creds.get("cloudsql").get("DB_NAME")
        DB_USER = creds.get("cloudsql").get("DB_USER")
        DB_PASSWORD = creds.get("cloudsql").get("DB_PASSWORD")
        DB_PORT = creds.get("cloudsql").get("DB_PORT")

        credentials = cls(DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT)
        return credentials
