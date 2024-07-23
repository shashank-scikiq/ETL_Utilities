import logging
import sys

def start_log():
    logger = logging.getLogger(__name__)
    if logger.handlers:
        return logger
    str_handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(levelname)s: %(asctime)s:[%(name)s]: %(message)s')
    str_handler.setFormatter(formatter)
    logger.addHandler(str_handler)
    logger.setLevel(logging.DEBUG)
    return logger


