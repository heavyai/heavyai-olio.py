from typing import Optional
import json
import os
import getpass
import subprocess
import sys
import re
from sqlalchemy.engine.url import make_url, URL
from pathlib import Path
import pandas as pd
import pymapd
import ibis.omniscidb
import pkg_resources
import omnisci_olio.pymapd
from omnisci_olio.pymapd import url_prompt


EXECUTION_TYPE_ICP = 1
EXECUTION_TYPE_ICP_GPU = 2
EXECUTION_TYPE_CURSOR = 3


class OmniSciIbisClient(ibis.omniscidb.OmniSciDBClient):

    # We can't yet pass a pymapd connection object to Ibis
    # and it does not support pymapd's binary TLS connections
    # so create a subclass for now.
    def __init__(
        self,
        uri=None,
        user=None,
        password=None,
        host=None,
        port=6274,
        database=None,
        protocol='binary',
        session_id=None,
        bin_cert_validate=None,
        bin_ca_certs=None,
        ipc: Optional[bool] = None,
        gpu_device: Optional[int] = None,
    ):
        """
        Parameters
        ----------
        uri : str
        user : str
        password : str
        host : str
        port : int
        database : str
        protocol : {'binary', 'http', 'https'}
        session_id: str
        """

        # We don't call super.__init__ because we connect differently with the cert params
        # super(OmniSciIbisClient, self).__init__(uri, user, password, host, port, database, protocol, session_id, ipc, gpu_device)

        self.uri = uri
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.db_name = database
        self.protocol = protocol
        self.session_id = session_id
        self.con = None # always create the attribute in case connect fails below

        self._check_execution_type(ipc=ipc, gpu_device=gpu_device)

        self.ipc = ipc
        self.gpu_device = gpu_device

        if session_id:
            if self.version < pkg_resources.parse_version('0.12.0'):
                raise PyMapDVersionError(
                    'Must have pymapd > 0.12 to use session ID'
                )
            self.con = pymapd.connect(
                uri=uri,
                host=host,
                port=port,
                protocol=protocol,
                sessionid=session_id,
                bin_cert_validate=bin_cert_validate,
                bin_ca_certs=bin_ca_certs,
            )
        else:
            self.con = pymapd.connect(
                uri=uri,
                user=user,
                password=password,
                host=host,
                port=port,
                dbname=database,
                protocol=protocol,
                bin_cert_validate=bin_cert_validate,
                bin_ca_certs=bin_ca_certs,
            )

     # Don't close the connection or we'll log the user out of Immerse
    def close(self):
        pass

def connect_prompt(url=None,
        username=None,
        password=None,
        host=None,
        port=None,
        database=None,
        protocol=None,
        lookup=None):
    """
    """

    u = url_prompt(url, username, password, host, port, database, protocol, lookup)
    # TODO BUG in ibis.omniscidb error passing the uri
    # con = ibis.omniscidb.connect(url_prompt(url))
    con = ibis.omniscidb.connect(user=u.username,
        password=u.password,
        host=u.host,
        port=u.port,
        database=u.database,
        protocol=u.query.get('protocol'),
    )
    return con


def connect(url=None,
        username=None,
        password=None,
        host=None,
        port=None,
        database=None,
        protocol=None,
        lookup=None):
    """
    """
    if not url and not username and not password \
            and not host and not port and not database \
            and not protocol \
            and not lookup and not 'OMNISCI_DB_URL' in os.environ:
        url = 'omnisci://admin:HyperInteractive@localhost:6274/omnisci'

    u = omnisci_olio.pymapd.url_prompt(url, username, password, host, port, database, protocol, lookup,
            param_prompt=omnisci_olio.pymapd.param_value)
    # TODO BUG in ibis.omniscidb error passing the uri
    # con = ibis.omniscidb.connect(url_prompt(url))
    con = ibis.omniscidb.connect(user=u.username,
        password=u.password,
        host=u.host,
        port=u.port,
        database=u.database,
        protocol=u.query.get('protocol'),
    )
    return con


def connect_session():
    session_path = '{0}/.jupyterscratch/.omniscisession'.format(Path.home())
    if os.path.exists(session_path):
        sessionid = json.loads(open(session_path, 'r').read()).get('session', 'invalid')
        bin_cert_validate = os.environ.get('OMNISCI_BINARY_TLS_VALIDATE', None)
        if bin_cert_validate is not None:
            bin_cert_validate = True if bin_cert_validate.lower() == 'true' else False
        return OmniSciIbisClient(
            session_id=sessionid,
            host=os.environ.get('OMNISCI_HOST'),
            port=os.environ.get('OMNISCI_PORT'),
            protocol=os.environ.get('OMNISCI_PROTOCOL'),
            bin_cert_validate=bin_cert_validate,
            bin_ca_certs=os.environ.get('OMNISCI_BINARY_TLS_CACERTS', None),
        )
    elif 'OMNISCI_DB_URL' in os.environ:
        return connect()
    else:
        raise Exception('Unable to connect automatically. No .omniscisession file found, no OMNISCI_DB_URL var set.')


# alias for implicit import in notebooks
omnisci_connect_prompt = connect_prompt


# alias for implicit import in notebooks
omnisci_connect = connect_session
