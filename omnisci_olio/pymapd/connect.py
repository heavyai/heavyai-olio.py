import json
import os
import getpass
import subprocess
import sys
from sqlalchemy.engine.url import make_url, URL
from pathlib import Path
import pandas as pd
import pymapd


def lookup_url_for_name(name):
    home = os.environ['HOME']
    for fname in [
            home + '/omnisci/omnisci-urls.csv',
            home + '/.omnisci/omnisci-urls.csv',
            home + '/host_home/omnisci/omnisci-urls.csv',
            home + '/host_home/.omnisci/omnisci-urls.csv',
            home + '/shared_notebooks/omnisci/omnisci-urls.csv']:
        if os.path.exists(fname):
            df = pd.read_csv(fname)
            s = df[df.name == name].url
            if not s.empty:
                return s.values[0]
    raise Exception('OmniSci connection url not found for name {}'.format(name))


def param_prompt(name, default_val, pw=False):
    if not pw:
        return input('OmniSciDB {} [{}]: '.format(name, default_val)) or default_val
    elif default_val is None:
        print('OmniSciDB password: ', end='')
        return getpass.getpass()
    else:
        return default_val


def param_value(name, default_val, pw=False):
    return default_val


def url_prompt(url=None,
        username=None,
        password=None,
        host=None,
        port=None,
        database=None,
        protocol=None,
        execution_type=None,
        ipc=None,
        lookup=None,
        param_prompt=param_prompt):
    """
    url: 'omnisci://admin:HyperInteractive@omniscidb.example.com:6274/omnisci?execution_type=3&protocol=binary'
    url: 'omnisci://admin@omniscidb.example.com:6274/omnisci?ipc=gpu'
    """

    if type(url) == URL:
        u = url
    elif url:
        u = make_url(url)
    elif lookup:
        u = make_url(lookup_url_for_name(lookup))
    elif 'OMNISCI_DB_URL' in os.environ:
        u = make_url(os.environ['OMNISCI_DB_URL'])
    else:
        u = URL('omnisci')

    if host or username or port:
        # if connecting to a different db instance (identified by host/user/port) discard the url password
        u.password = None

    print(u.__repr__())

    if host: u.host = host
    elif not u.host: u.host = param_prompt('host', 'localhost')

    if port: u.port = port
    elif not u.port: u.port = int(param_prompt('port', '6274'))

    if database: u.database = database
    elif not u.database: u.database = param_prompt('database', 'omnisci')

    if protocol: u.query['protocol'] = protocol
    elif not u.query.get('protocol'):
        u.query['protocol'] = 'binary'

    ipc_dict = {'cpu': '1', 'gpu': '2', 'remote': '3', None: '3'}
    if ipc: u.query['execution_type'] = ipc_dict[ipc]
    elif u.query.get('ipc'):
        u.query['execution_type'] = ipc_dict[u.query['ipc']]
        del u.query['ipc']

    if execution_type: u.query['execution_type'] = execution_type
    elif u.query.get('execution_type'):
        # TODO fix in ibis.omniscidb.connect: cast str to int
        u.query['execution_type'] = int(u.query['execution_type'])
    else:
        u.query['execution_type'] = ipc_dict[None]
    # else:
    #     # u.query['execution_type'] = int(param_prompt('execution_type', '3'))
    #     u.query['execution_type'] = ipc_dict[param_prompt('ipc (one of {})'.format(ipc_dict.keys), 'remote')]

    if username: u.username = username
    elif not u.username:
        if 'JUPYTERHUB_USER' in os.environ:
            u.username = os.environ['JUPYTERHUB_USER']
        else:
            u.username = param_prompt('username', 'admin')

    if password: u.password_original = password
    elif not u.password:
        u.password_original = param_prompt('password', None, pw=True)

    print(u.__repr__())
    return u


def connect_prompt(url=None,
        username=None,
        password=None,
        host=None,
        port=None,
        database=None,
        protocol=None,
        execution_type=None,
        ipc=None,
        lookup=None):
    """
    """
    u = url_prompt(url, username, password, host, port, database, protocol, execution_type, ipc, lookup)
    return pymapd.connect(u)


def connect():
    return pymapd.connect(os.environ['OMNISCI_DB_URL'])
