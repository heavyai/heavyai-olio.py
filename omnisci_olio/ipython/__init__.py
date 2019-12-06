"""OmniSciDB SQL magic"""

# https://ipython.readthedocs.io/en/stable/config/custommagics.html

from .magic import OmniSciSqlMagic

def load_ipython_extension(ipython):
    ipython.register_magics(OmniSciSqlMagic)
