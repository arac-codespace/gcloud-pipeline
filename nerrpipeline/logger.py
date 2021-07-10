from logging import Logger, StreamHandler, Formatter, getLogger, DEBUG
from logging.handlers import TimedRotatingFileHandler
import sys
import os
from app_constants import LOGS_FOLDER


class AppLogger:

    # https://www.toptal.com/python/in-depth-python-logging
    fmt = "%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s"
    FILE_FORMATTER = Formatter(fmt=fmt, datefmt="%m/%d/%Y %H:%M:%S")

    fmt = "%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s"
    CONSOLE_FORMATTER = Formatter(fmt=fmt, datefmt="%m/%d/%Y %H:%M:%S")

    @classmethod
    def default_logger(cls, logger_name: str, log_file: str = "app_logging.log") -> Logger:
        applogger = cls()
        logger = applogger.get_logger(logger_name, log_file)
        return logger

    def set_log_path(self, log_file: str) -> str:
        log_file = os.path.join(LOGS_FOLDER, log_file)
        return log_file

    def get_console_handler(self, formatter: Formatter = CONSOLE_FORMATTER) -> StreamHandler:
        console_handler = StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        return console_handler

    def get_file_handler(
        self,
        log_file: str,
        formatter: Formatter = FILE_FORMATTER
    ) -> TimedRotatingFileHandler:

        log_file = self.set_log_path(log_file)
        file_handler = TimedRotatingFileHandler(log_file, when='W6')
        file_handler.setFormatter(formatter)
        return file_handler

    # Best practice is to use __name__
    # in the calling module
    def get_logger(self, logger_name: str, log_file: str) -> Logger:
        logger = getLogger(logger_name)
        logger.setLevel(DEBUG)
        console_handler = self.get_console_handler()
        logger.addHandler(console_handler)
        file_handler = self.get_file_handler(log_file)
        logger.addHandler(file_handler)
        logger.propagate = False
        return logger
