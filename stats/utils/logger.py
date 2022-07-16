import logging


def get_logger(logger_name: str, log_level: str = "info") -> logging.Logger:
    """
    Setup of the logger for the function
    :param logger_name: name of the logger
    :param log_level: Level for the logging
    :return: logging.Logger that will work for this function
    """
    logger = logging.getLogger(logger_name)
    #logger.setLevel(logging.DEBUG)
    logger.setLevel(log_level.upper())
    ch = logging.StreamHandler()
    #ch.setLevel(logging.DEBUG)
    ch.setLevel(log_level.upper())
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger
