import logging
import re
import string
import unicodedata


def get_pipeline_logger(module_name: str) -> logging.Logger:
    """Return a child logger of the root 'pipeline' logger.

    All configuration (handlers, level, formatters) is set once in main.py via
    setup_logger(). Child loggers inherit that config automatically through
    propagation.  Use as:
        logger = get_pipeline_logger(__name__)   # → pipeline.data_pipeline.loader
    or with a short name:
        logger = get_pipeline_logger("loader")   # → pipeline.loader
    """
    return logging.getLogger(f"pipeline.{module_name}")


def manage_logger(logfile_name: str) -> logging.Logger:
    """Deprecated — use get_pipeline_logger() instead.

    Kept for backward compatibility during the transition. Returns the root
    pipeline logger regardless of the logfile_name argument (the file handler
    is managed centrally in main.py).
    """
    return logging.getLogger("pipeline")

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
