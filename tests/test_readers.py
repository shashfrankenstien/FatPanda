import pandas as pd
import sqlite3

import fatpanda as fpd
from tests import setup_fixture


def test_read_csv(setup_fixture):
    setup, test_file_name = setup_fixture
    df = fpd.read_csv(test_file_name)
    assert(setup.equals(df.read_into_mem()))


def test_concat_csv(setup_fixture):
    setup, test_file_name = setup_fixture
    filenames = [test_file_name, test_file_name]
    for f in filenames:
        setup.to_csv(f, index=False)
        setup.to_csv(f, index=False)
    df = fpd.concat_csv(filenames)
    setup_concat = pd.concat([setup, setup])
    assert(setup_concat.equals(df.read_into_mem()))


def test_read_sql_query(setup_fixture):
    _, test_file_name = setup_fixture
    csvdf = fpd.read_csv(test_file_name)

    con = sqlite3.connect(fpd.SQLITE_NAME)
    df = fpd.read_sql_query(f"select * from {csvdf.tablename}", con)
    con.close()
    assert(csvdf.read_into_mem().equals(df.read_into_mem()))
