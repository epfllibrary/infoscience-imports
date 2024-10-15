import logging

def manage_logger(logfile_name):
    # Configure logging for a specific logger (unique per instance)
    logger = logging.getLogger(logfile_name)
    logger.setLevel(logging.DEBUG)  # Set the logging level to DEBUG

    # Check if the logger already has handlers to avoid adding multiple handlers
    if not logger.handlers:  # This ensures we don't add handlers multiple times
        # Create a file handler
        file_handler = logging.FileHandler(logfile_name)
        file_handler.setLevel(
            logging.DEBUG
        )  # Set the level for the file handler to DEBUG

        # Create a console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(
            logging.INFO
        )  # Set the level for the console handler to INFO

        # Create a formatter and set it for both handlers
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        # Add the handlers to the logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    # Prevent log messages from propagating to the root logger
    logger.propagate = False

    return logger
