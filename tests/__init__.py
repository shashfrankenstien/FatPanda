import pytest
from io import StringIO
import os

import pandas as pd
import fatpanda as fpd

csv_file = '''A,B,C,D
0,1,2,shashank
1,2,5,potato
,5,3
2,1,89,man
0,1,2,man
1,17,10
1,18,10
1,19,10
,5,2
2,1,89'''

@pytest.fixture(scope='module')
def setup_fixture():
    df = pd.read_csv(StringIO(csv_file))
    test_file_name = "test_demo.csv"
    df.to_csv(test_file_name, index=False)
    fdf = fpd.read_csv(test_file_name)
    yield df, fdf, test_file_name
    if os.path.isfile(test_file_name):
        os.remove(test_file_name)

def _read_into_mem(df):
    if hasattr(df, 'read_into_mem'):
        return df.read_into_mem()
    else:
        return df

def assert_df_equal(a, b):
    a = _read_into_mem(a)
    b = _read_into_mem(b)
    if isinstance(b, pd.DataFrame):
        b = b[list(a)] # Ensure column order is same
    assert ((a == b) | ((a != a) & (b != b))).values.all() == True