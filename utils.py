import logging

def manage_logger(logfile_name):
    # Configure logging
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)  # Set the logging level

    # Create a file handler
    file_handler = logging.FileHandler(logfile_name)  # Specify the log file name
    file_handler.setLevel(logging.DEBUG)  # Set the level for the file handler

    # Create a formatter and set it for the file handler
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)

    # Add the file handler to the logger
    logger.addHandler(file_handler)
    return logger