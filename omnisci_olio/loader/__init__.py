"""OmniSciDB loader"""

__version__ = '0.1.0'

from .loader import omnisci_states, omnisci_counties, omnisci_countries, omnisci_log

from .logs_sql import read_log_file_sql
