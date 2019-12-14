"""
Example use of OmniSci UDF and UDTF using python and pymapd.

Instructions:

If you use conda environment, create with:

    conda create -y -n omnisci-example python=3.7
    conda activate omnisci-example

In whatever python environment you have:

    pip install "pymapd==0.18"

Set your connection parameters in a hidden place:

    export OMNISCI_DB_URL="omnisci://admin:HyperInteractive@omnisciserver:6274/omnisci"

Then run this example:

    python test_udf_udtf.py

"""


import os
import pymapd
import pandas as pd


con = pymapd.connect(os.environ['OMNISCI_DB_URL'])
print(con)


# ## Define Functions
# 

# Use `omnisci_udf` for the function annotations because it is more clear than `con`.
# 

omnisci_udf = con


# ### UDF definitions
# 
# UDF can be overloaded to work with different column datatypes.
# 


@omnisci_udf('double(double)')
def incr(x):
    return x + 1.0


@omnisci_udf('int32(int32)')
def incr(x):
    return x + 1


@omnisci_udf('int64(int64)')
def incr(x):
    return x + 10


# ### UDTF definitions
# 


@omnisci_udf('int32|table(double*|cursor, int32*|input, int64*, int64*, double*|output)')
def example_row_copier(x,
                   m_ptr: dict(sizer='kUserSpecifiedRowMultiplier'),
                   input_row_count_ptr, output_row_count, y):
    m = m_ptr[0]
    input_row_count = input_row_count_ptr[0]
    for i in range(input_row_count):
        for c in range(m):
            j = i + c * input_row_count
            y[j] = x[i] * 2
    output_row_count[0] = m * input_row_count
    return 0


# ## Invoke UDF on Sample Data
# 
# The "omnisci_countries" is included with OmniSci DB as a sample dataset.
# We'll use this because it has some int and float columns.
# 

tname = 'omnisci_countries'

# Helper function for tests
def assert_lines_equal(expected, actual):
    for exp, act in zip(expected.strip().split('\n'), actual.strip().split('\n')):
        assert exp.strip() == act.strip()

def test_sql_udf():
    print('SQL to invoke a UDF')

    df = pd.read_sql(f"""
    select gdp_md_est, incr(gdp_md_est) gdp_md_est_incr,
    scalerank, incr(scalerank) scalerank_incr
    from {tname}
    order by gdp_md_est
    limit 5
    """, con)
    print(df.to_csv())

    expected = """
        ,gdp_md_est,gdp_md_est_incr,scalerank,scalerank_incr
        0,-99.0,-98.0,1,2
        1,16.0,17.0,3,4
        2,105.1,106.1,1,2
        3,760.4,761.4,1,2
        4,904.2,905.2,1,2
    """
    assert_lines_equal(expected, df.to_csv())


# Invoke UDTF on Sample Data
# 
def test_sql_udtf():
    print('SQL to invoke a UDTF')

    df = pd.read_sql(f"""
    select gdp_md_est, incr(gdp_md_est) gdp_md_est_incr
    from table(example_row_copier(cursor(select gdp_md_est from {tname}), 2))
    order by gdp_md_est
    limit 5
    """, con)
    print(df.to_csv())

    expected = """
        ,gdp_md_est,gdp_md_est_incr
        0,-198.0,-197.0
        1,-198.0,-197.0
        2,32.0,33.0
        3,32.0,33.0
        4,210.2,211.2
    """
    assert_lines_equal(expected, df.to_csv())


if __name__ == "__main__":
    test_sql_udf()
    test_sql_udtf()
