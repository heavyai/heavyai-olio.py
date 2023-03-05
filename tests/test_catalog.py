from heavyai_olio.ibis import connect
import heavyai_olio.catalog as cat
import pytest

def test_heavyai_states():
    tname = "heavyai_states"
    with connect() as con:
        t = cat.heavyai_states(con, drop=True)
        assert tname == t.name
        assert 52 == t.count().execute()


def test_heavyai_counties():
    tname = "heavyai_counties"
    with connect() as con:
        t = cat.heavyai_counties(con, drop=True)
        assert tname == t.name
        assert 3236 == t.count().execute()


def test_heavyai_countries():
    tname = "heavyai_countries"
    with connect() as con:
        t = cat.heavyai_countries(con, drop=True)
        assert tname == t.name
        assert 177 == t.count().execute()


@pytest.mark.skip(reason="/storage not accessible")
def test_omnisci_log():
    tname = "heavyai_log"
    with connect() as con:
        t = cat.heavydb_log(con, drop=True, src_pattern="heavydb.INFO")
        assert tname == t.name
        assert 6 <= t.count().execute()
