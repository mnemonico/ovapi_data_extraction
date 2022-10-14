import logging


def usecase_logger(filename):
    """
    return a logger
    :param filename:
    :return:
    """
    logger = logging.getLogger(filename)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    return logger


if __name__ == '__main__':
    pass

