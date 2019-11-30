"""OmniSciDB SQL magic"""

# https://ipython.readthedocs.io/en/stable/config/custommagics.html

__version__ = '0.1.0'

from .magic import OmniSciSqlMagic

def load_ipython_extension(ipython):
    ipython.register_magics(OmniSciSqlMagic)
