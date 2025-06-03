# logger_setup.py
import logging
import os
from logging.handlers import RotatingFileHandler

def setup_logger(log_file_path='logs/sqlagent.log', log_level=logging.INFO):
    """Sets up a rotating file logger."""
    log_dir = os.path.dirname(log_file_path)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)

    logger = logging.getLogger('SQLAgent')
    logger.setLevel(log_level)

    # Prevent duplicate handlers if called multiple times
    if logger.hasHandlers():
        logger.handlers.clear()

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # File Handler
    fh = RotatingFileHandler(log_file_path, maxBytes=5*1024*1024, backupCount=2) # 5MB per file, 2 backups
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    # Console Handler (optional, for debugging)
    # ch = logging.StreamHandler()
    # ch.setFormatter(formatter)
    # logger.addHandler(ch)

    return logger

# Initialize logger instance for global use if desired, or get it on demand
# logger = setup_logger()