"""OmniSci Olio: various python functions for use with OmniSci DB, using Pymapd and Ibis"""

import logging

# Define root logger
handler = logging.StreamHandler()
format = '%(asctime)s | %(levelname)s | %(name)s | %(message)s'
datefmt = '%Y-%m-%d %H:%M:%S'
logging.basicConfig(level=logging.INFO, handlers=[handler], format=format, datefmt=datefmt)
