import logging
import sys

def setup_logging(level=logging.INFO):
    """
    Configures basic logging.
    """
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout) # Ensure logs go to stdout
        ]
    )

def get_logger(name):
    """
    Returns a logger instance.
    """
    return logging.getLogger(name)

# Initial setup for any module that imports this
setup_logging()
