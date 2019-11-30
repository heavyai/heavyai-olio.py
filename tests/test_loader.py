from omnisci_olio.ibis import connect
import omnisci_olio.loader as old

def test_omnisci_states():
    tname = 'omnisci_states'
    with connect() as con:
        t = old.omnisci_states(con, drop=True)
        assert tname == t.name
        assert 52 == t.count().execute()

def test_omnisci_counties():
    tname = 'omnisci_counties'
    with connect() as con:
        t = old.omnisci_counties(con, drop=True)
        assert tname == t.name
        assert 3250 == t.count().execute()

def test_omnisci_countries():
    tname = 'omnisci_countries'
    with connect() as con:
        t = old.omnisci_countries(con, drop=True)
        assert tname == t.name
        assert 177 == t.count().execute()

def test_omnisci_log():
    tname = 'omnisci_log'
    with connect() as con:
        t = old.omnisci_log(con, drop=True, src_pattern='omnisci_server.INFO')
        assert tname == t.name
        assert 6 <= t.count().execute()
