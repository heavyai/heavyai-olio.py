from omnisci_olio.ibis import connect
import omnisci_olio.catalog as cat
import pytest

def test_omnisci_states():
    tname = 'omnisci_states'
    with connect() as con:
        t = cat.omnisci_states(con, drop=True)
        assert tname == t.name
        assert 52 == t.count().execute()

def test_omnisci_counties():
    tname = 'omnisci_counties'
    with connect() as con:
        t = cat.omnisci_counties(con, drop=True)
        assert tname == t.name
        assert 3236 == t.count().execute()

def test_omnisci_countries():
    tname = 'omnisci_countries'
    with connect() as con:
        t = cat.omnisci_countries(con, drop=True)
        assert tname == t.name
        assert 177 == t.count().execute()

@pytest.mark.skip(reason="/omnisci-storage not accessible")
def test_omnisci_log():
    tname = 'omnisci_log'
    with connect() as con:
        t = cat.omnisci_log(con, drop=True, src_pattern='omnisci_server.INFO')
        assert tname == t.name
        assert 6 <= t.count().execute()
