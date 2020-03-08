import pandas as pd
import sqlite3

import fatpanda as fpd
from tests import setup_fixture, assert_df_equal


def test_read_csv(setup_fixture):
    setup, _, test_file_name = setup_fixture
    df = fpd.read_csv(test_file_name)
    assert_df_equal(setup, df)


def test_concat_csv(setup_fixture):
    setup, _, test_file_name = setup_fixture
    filenames = [test_file_name, test_file_name]
    for f in filenames:
        setup.to_csv(f, index=False)
        setup.to_csv(f, index=False)
    df = fpd.concat_csv(filenames)
    setup_concat = pd.concat([setup, setup])
    assert_df_equal(setup_concat, df)


def test_read_sql_query(setup_fixture):
    setup, csvdf, _ = setup_fixture

    con = sqlite3.connect(fpd.SQLITE_NAME)
    df = fpd.read_sql_query(f"select * from {csvdf.tablename}", con)
    con.close()
    assert_df_equal(csvdf, df)
    assert_df_equal(setup, df)
