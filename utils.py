import logging

def manage_logger(logfile_name):
    # Configure the root logger to use a NullHandler
    logging.getLogger().addHandler(logging.NullHandler())

    # Configure logging for this specific logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)  # Set the logging level to DEBUG

    # Create a file handler
    file_handler = logging.FileHandler(logfile_name)  # Specify the log file name
    file_handler.setLevel(logging.DEBUG)  # Set the level for the file handler to DEBUG

    # Create a formatter and set it for the file handler
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)

    # Add the file handler to the logger
    logger.addHandler(file_handler)

    return logger