"""Logging configuration for the application."""

import logging
import sys

# Create logger
logger = logging.getLogger("stables-analytics")
logger.setLevel(logging.DEBUG)

# Console handler with formatting
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)

# Format with timestamp and level
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
console_handler.setFormatter(formatter)

# Add handler to logger
logger.addHandler(console_handler)
