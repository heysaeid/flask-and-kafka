import logging
from flask_and_kafka.formatters import ProducerFormatter 
from flask_and_kafka.formatters import ConsumerFormatter


def setup_logger(formatter: logging.Formatter, name: str, file: str, level=logging.INFO):
    handler = logging.FileHandler(file)        
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger


def consumer_logger(**kwargs) -> setup_logger:
    formatter = ConsumerFormatter('%(asctime)s %(name)s %(threadName)s : %(message)s')
    return setup_logger(formatter=formatter, **kwargs)


def producer_logger(**kwargs) -> setup_logger:
    formatter = ProducerFormatter('%(asctime)s %(name)s %(threadName)s : %(message)s')
    return setup_logger(formatter=formatter, **kwargs)
