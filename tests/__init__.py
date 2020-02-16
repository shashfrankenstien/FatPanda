import pytest
import pandas  as pd
from io import StringIO
import os

csv_file = '''A,B,C,D
0,1,2,shashank
1,2,5,potato
,5,3
2,1,89,man
0,1,2
1,17,10
,5,2
2,1,89'''

@pytest.fixture(scope='module')
def setup_fixture():
    df = pd.read_csv(StringIO(csv_file))
    test_file_name = "test_demo.csv"
    df.to_csv(test_file_name, index=False)
    yield df, test_file_name
    if os.path.isfile(test_file_name):
        os.remove(test_file_name)
