import os

# Directory for log files
LOG_DIR = os.path.join(os.getcwd(), "logs")

# Maximum size of a log file in bytes (e.g., 5 MB)
LOG_FILE_SIZE = 5 * 1024 * 1024

# Number of backup log files to keep
LOG_BACKUP_COUNT = 5

# Date and time formats
DATE_FORMAT = "%Y-%m-%d"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

# Directory for temporary files
TEMP_DIR = os.path.join(os.getcwd(), "temp")

# Path for error logs
ERROR_LOG_FILE = os.path.join(LOG_DIR, "error.log")
