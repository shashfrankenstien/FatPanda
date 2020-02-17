import pandas  as pd

import fatpanda as fpd
from tests import setup_fixture, assert_df_equal



def test_getter_column_name(setup_fixture):
    setup, test_file_name = setup_fixture
    df = fpd.read_csv(test_file_name)
    A = df["A"]
    assert(isinstance(A, fpd.core._Series))
    assert_df_equal(setup['A'], A)


def test_getter_columns(setup_fixture):
    setup, test_file_name = setup_fixture
    df = fpd.read_csv(test_file_name)
    cols = ['A', 'B']
    assert_df_equal(setup[cols], df[cols])


def test_getter_virtual(setup_fixture):
    setup, test_file_name = setup_fixture
    setup_df = setup.copy() # setting items on copy
    df = fpd.read_csv(test_file_name)
    setup_df['K'] = 20
    df['K'] = 20
    assert(isinstance(df['K'], fpd.core._VirtualSeries))
    assert_df_equal(setup_df["K"], df['K'])


def test_head(setup_fixture):
    setup, test_file_name = setup_fixture
    df = fpd.read_csv(test_file_name)
    assert_df_equal(setup.head(), df.head())
    assert_df_equal(setup.head(2), df.head(2))


def test_getter_mask(setup_fixture):
    setup, test_file_name = setup_fixture
    df = fpd.read_csv(test_file_name)

    pd_mask = setup['D'] == 'man'
    fpd_mask = df['D'] == 'man'
    assert_df_equal(setup[pd_mask], df[fpd_mask])

    pd_mask = setup['B'] == 5
    fpd_mask = df['B'] == 5
    assert_df_equal(setup[pd_mask], df[fpd_mask])

    pd_mask = setup['B'] != 5
    fpd_mask = df['B'] != 5
    assert_df_equal(setup[pd_mask], df[fpd_mask])

    pd_mask = setup['B'] > 5
    fpd_mask = df['B'] > 5
    assert_df_equal(setup[pd_mask], df[fpd_mask])

    pd_mask = setup['B'] >= 5
    fpd_mask = df['B'] >= 5
    assert_df_equal(setup[pd_mask], df[fpd_mask])

    pd_mask = setup['B'] < 5
    fpd_mask = df['B'] < 5
    assert_df_equal(setup[pd_mask], df[fpd_mask])

    pd_mask = setup['B'] <= 5
    fpd_mask = df['B'] <= 5
    assert_df_equal(setup[pd_mask], df[fpd_mask])

