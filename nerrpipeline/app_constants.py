import os

from utility import create_subdirectories

# Folder structure constants
DIR_PATH = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CREDENTIAL_FILE = os.path.join(DIR_PATH, "credentials.json")
LOGS_FOLDER = create_subdirectories(os.path.join(DIR_PATH, "logs"))
