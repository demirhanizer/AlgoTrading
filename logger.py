import os
import logging
from logging.handlers import RotatingFileHandler

# Define log directory
log_dir = os.getenv("LOG_DIR", "logs")  # Use "logs" locally, "/app/logs" in Docker

# Ensure log directory exists
if not os.path.exists(log_dir):
    os.makedirs(log_dir, exist_ok=True)

# Setup logging
log_file = os.path.join(log_dir, "trading_bot.log")
log_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

# Rotating File Handler (keeps logs limited in size)
file_handler = RotatingFileHandler(log_file, maxBytes=5 * 1024 * 1024, backupCount=3)
file_handler.setFormatter(log_formatter)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)

logger.info("🚀 Logging initialized successfully!")
