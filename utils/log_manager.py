import logging

# Configure logger (levels, handlers, formatters)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Basic handler and formatter
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(module)s:%(lineno)d - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def get_logger():
    return logger