import logging
import re
import string
import unicodedata

def manage_logger(logfile_name):
    # Configure logging for a specific logger (unique per instance)
    logger = logging.getLogger(logfile_name)
    logger.setLevel(logging.INFO)  # Set the logging level to DEBUG

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
            logging.DEBUG
        )  # Set the level for the console handler to INFO

        # Create a formatter and set it for both handlers
        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s"
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        # Add the handlers to the logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    # Prevent log messages from propagating to the root logger
    logger.propagate = False

    return logger

def clean_value(formatted_name):
    formatted_name = formatted_name.lower()

    # Replace dash-like characters between initials or names with space
    formatted_name = re.sub(r"[-‐‑‒–—―⁃﹘﹣－]", " ", formatted_name)

    # Separate joined initials (e.g., J.-L. → J L)
    formatted_name = re.sub(r"\b([A-Z])\.\-?([A-Z])\.\b", r"\1 \2", formatted_name)

    # Remove remaining periods (e.g., J. → J)
    formatted_name = formatted_name.replace(".", " ")

    # Remove any leftover punctuation
    formatted_name = formatted_name.translate(
        str.maketrans("", "", string.punctuation)
    )
    # Normalize whitespace
    formatted_name = re.sub(r"\s+", " ", formatted_name).strip()

    return formatted_name


def normalize_title(title):
    lowercase_words = {
        "and",
        "or",
        "but",
        "a",
        "an",
        "the",
        "as",
        "at",
        "by",
        "for",
        "in",
        "of",
        "on",
        "to",
        "up",
        "with",
    }

    words = title.lower().split()

    normalized_title = [words[0].capitalize()] + [
        word if word in lowercase_words else word.capitalize() for word in words[1:]
    ]

    return " ".join(normalized_title)
