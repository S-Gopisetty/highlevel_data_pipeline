from datetime import datetime
import os
import logging

def setup_dual_logger(name: str, log_dir: str = "logs") -> logging.Logger:
    """
    Creates a logger that logs both to console (Airflow) and to a file in `logs/`.

    :param name: Base name of the log file (without extension)
    :param log_dir: Directory to store log files
    :return: Configured logger instance
    """
    os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Avoid duplicate handlers
    if not logger.handlers:
        formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # File handler with timestamp
        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        log_file_path = os.path.join(log_dir, f"{name}_{timestamp}.log")
        file_handler = logging.FileHandler(log_file_path, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
