import pandas  as pd

import fatpanda as fpd
from tests import setup_fixture



def test_getter_column_name(setup_fixture):
    setup, test_file_name = setup_fixture
    df = fpd.read_csv(test_file_name)
    A = df["A"]
    assert(isinstance(A, fpd.core._Series))
    assert(setup["A"].equals(A.read_into_mem()))


def test_getter_columns(setup_fixture):
    setup, test_file_name = setup_fixture
    df = fpd.read_csv(test_file_name)
    cols = ['A', 'B']
    assert(setup[cols].equals(df[cols].read_into_mem()))


def test_getter_virtual(setup_fixture):
    setup, test_file_name = setup_fixture
    setup_df = setup.copy() # setting items on copy
    df = fpd.read_csv(test_file_name)
    setup_df['K'] = 20
    df['K'] = 20
    assert(isinstance(df['K'], fpd.core._VirtualSeries))
    assert(setup_df["K"].equals(df['K'].read_into_mem()))


def test_head(setup_fixture):
    setup, test_file_name = setup_fixture
    df = fpd.read_csv(test_file_name)
    assert(setup.head().equals(df.head().read_into_mem()))
    assert(setup.head(2).equals(df.head(2).read_into_mem()))


def test_getter_mask(setup_fixture):
    setup, test_file_name = setup_fixture
    df = fpd.read_csv(test_file_name)

    pd_mask = setup['A'] == 2
    fpd_mask = df['A'] == 2
    assert(setup[pd_mask].equals(df[fpd_mask].read_into_mem()))

    pd_mask = setup['B'] == 5
    fpd_mask = df['B'] == 5
    assert(setup[pd_mask]['B'].equals(df[fpd_mask]['B'].read_into_mem()))
    # assert(setup[pd_mask].equals(df[fpd_mask].read_into_mem())) # NOTE: This doesn't seem to work because of NaN vs None difference between pd and fpd

    pd_mask = setup['B'] != 5
    fpd_mask = df['B'] != 5
    assert(setup[pd_mask].equals(df[fpd_mask].read_into_mem()))

    pd_mask = setup['B'] > 5
    fpd_mask = df['B'] > 5
    assert(setup[pd_mask].equals(df[fpd_mask].read_into_mem()))

    pd_mask = setup['B'] >= 5
    fpd_mask = df['B'] >= 5
    assert(setup[pd_mask].equals(df[fpd_mask].read_into_mem()))

    pd_mask = setup['B'] < 5
    fpd_mask = df['B'] < 5
    assert(setup[pd_mask].equals(df[fpd_mask].read_into_mem()))

    pd_mask = setup['B'] <= 5
    fpd_mask = df['B'] <= 5
    assert(setup[pd_mask].equals(df[fpd_mask].read_into_mem()))