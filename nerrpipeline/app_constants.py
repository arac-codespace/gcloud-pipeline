import os

from utility import create_subdirectories

# Folder structure constants
DIR_PATH = os.path.dirname(os.path.abspath(__file__))
LOGS_FOLDER = create_subdirectories(os.path.join(DIR_PATH, "logs"))
