"""HeavyaiDB SQL magic"""

# https://ipython.readthedocs.io/en/stable/config/custommagics.html

from .magic import HeavyaiSqlMagic


def load_ipython_extension(ipython):
    ipython.register_magics(HeavyaiSqlMagic)
