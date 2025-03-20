import logging
import string
import unicodedata

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

def clean_value(value):
    value = value.lower()
    value = value.translate(
        str.maketrans(string.punctuation, " " * len(string.punctuation))
    )
    value = remove_accents(value)
    value = value.encode("ascii", "ignore").decode("utf-8")
    return value


def remove_accents(input_str):
    replacements = {
        "ø": "o",
        "ä": "ae",
        "ö": "oe",
        "ü": "ue",
        "ß": "ss",
        "å": "aa",
        "é": "e",
        "è": "e",
        "ê": "e",
        "æ": "ae",
        "œ": "oe",
    }

    for original, replacement in replacements.items():
        input_str = input_str.replace(original, replacement)

    nfkd_form = unicodedata.normalize("NFKD", input_str)
    cleaned_str = "".join([c for c in nfkd_form if not unicodedata.combining(c)])

    return cleaned_str


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
