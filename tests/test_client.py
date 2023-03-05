from heavyai_olio.workflow import connect
import heavyai_olio.catalog as cat


def test_heavyai_counties():
    tname = "heavyai_counties"
    with connect() as con:
        t = con.table(tname)
        assert tname == t.name
        assert 3236 == t.count().execute()


def test_cross_counties():
    tname = "heavyai_counties"
    with connect() as con:
        t = con.table(tname)
        u = t.view()
        j = t.join(u, t["fips"] == u["fips"])
        cols = [c for c in t.columns]
        print(cols)
        p = j.select(
            [t[c].name("a_" + c) for c in cols] + [u[c].name("b_" + c) for c in cols]
        )
        tn = con.store(p, "test_cross_counties")

        ct = con.table(tn).count().execute()
        assert 3236 <= ct
