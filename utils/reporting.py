import logging
import sys


def start_logging(level=logging.DEBUG):
    logging.basicConfig(stream=sys.stdout, level=level, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # create console handler and set level to debug
    # ch = logging.StreamHandler()
    # ch.setLevel(logging.DEBUG)
    # ch.setFormatter(formatter)
