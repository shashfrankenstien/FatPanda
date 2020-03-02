import pandas  as pd

import fatpanda as fpd
from tests import setup_fixture, assert_df_equal



def test_arith_columns(setup_fixture):
    setup, _, test_file_name = setup_fixture
    setup_df = setup.copy() # setting items on a copy
    df = fpd.read_csv(test_file_name)

    setup_df['Add'] = setup_df['B'] + setup_df['C']
    df['Add'] = df['B'] + df['C']
    assert_df_equal(setup_df["Add"], df['Add'])

    setup_df['Sub'] = setup_df['Add'] - setup_df['C']
    df['Sub'] = df['Add'] - df['C']
    assert_df_equal(setup_df["Sub"], df['Sub'])

    setup_df['Div'] = setup_df['Sub'] / setup_df['C']
    df['Div'] = df['Sub'] / df['C']
    assert_df_equal(setup_df["Div"], df['Div'])

    setup_df['Mult'] = setup_df['Div'] * setup_df['C']
    df['Mult'] = df['Div'] * df['C']
    assert_df_equal(setup_df["Mult"], df['Mult'])

    setup_df['Mod'] = setup_df['Mult'] % setup_df['C']
    df['Mod'] = df['Mult'] % df['C']
    assert_df_equal(setup_df["Mod"], df['Mod'])

    setup_df['A'] = setup_df['A'] + setup_df['A']
    df['A'] = df['A'] + df['A']
    assert_df_equal(setup_df["A"], df['A'])

    setup_df['Y'] = setup_df['B']
    df['Y'] = df['B']
    assert_df_equal(setup_df["Y"], df['Y'])

    setup_df['Z'] = setup_df['Div']
    df['Z'] = df['Div']
    assert_df_equal(setup_df["Z"], df['Z'])

    assert_df_equal(setup_df, df)


def test_constants(setup_fixture):
    setup, _, test_file_name = setup_fixture
    setup_df = setup.copy() # setting items on a copy
    df = fpd.read_csv(test_file_name)

    setup_df['const_int'] = 20
    df['const_int'] = 20

    setup_df['const_str'] = "test_string"
    df['const_str'] = "test_string"

    # setup_df['const_bool'] = True
    # df['const_bool'] = True
    # assert(setup_df.equals(df.read_into_mem()))
    assert_df_equal(setup_df, df)


def test_arith_constants(setup_fixture):
    setup, _, test_file_name = setup_fixture
    setup_df = setup.copy() # setting items on a copy
    df = fpd.read_csv(test_file_name)

    setup_df['const_int'] = 20
    setup_df['P'] = (setup_df['B'] + 1) * setup_df['const_int'] % 5
    df['const_int'] = 20
    df['P'] = (df['B'] + 1) * df['const_int'] % 5

    assert_df_equal(setup_df, df)
