"""HEAVYAI Olio: various python functions for use with HEAVY DB, using the heavyai and Ibis python libraries"""

import logging

# Define root logger
handler = logging.StreamHandler()
format = '%(asctime)s | %(levelname)s | %(name)s | %(message)s'
datefmt = '%Y-%m-%d %H:%M:%S'
logging.basicConfig(level=logging.INFO, handlers=[handler], format=format, datefmt=datefmt)
