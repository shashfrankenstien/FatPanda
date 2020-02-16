import pandas  as pd

import fatpanda as fpd
from tests import setup_fixture



def test_arith_columns(setup_fixture):
    setup, test_file_name = setup_fixture
    df = fpd.read_csv(test_file_name)
    setup_df = setup.copy() # setting items on copy

    setup_df['Add'] = setup_df['B'] + setup_df['C']
    df['Add'] = df['B'] + df['C']
    assert(setup_df["Add"].equals(df['Add'].read_into_mem()))

    setup_df['Sub'] = setup_df['Add'] - setup_df['C']
    df['Sub'] = df['Add'] - df['C']
    assert(setup_df["Sub"].equals(df['Sub'].read_into_mem()))

    setup_df['Div'] = setup_df['Sub'] / setup_df['C']
    df['Div'] = df['Sub'] / df['C']
    assert(setup_df["Div"].equals(df['Div'].read_into_mem()))

    setup_df['Mult'] = setup_df['Div'] * setup_df['C']
    df['Mult'] = df['Div'] * df['C']
    assert(setup_df["Mult"].equals(df['Mult'].read_into_mem()))

    setup_df['Mod'] = setup_df['Mult'] % setup_df['C']
    df['Mod'] = df['Mult'] % df['C']
    assert(setup_df["Mod"].equals(df['Mod'].read_into_mem()))

    setup_df['A'] = setup_df['A'] + setup_df['A']
    df['A'] = df['A'] + df['A']
    assert(setup_df["A"].equals(df['A'].read_into_mem()))

    setup_df['Z'] = setup_df['A']
    df['Z'] = df['A']
    assert(setup_df["Z"].equals(df['Z'].read_into_mem()))


def test_constants(setup_fixture):
    setup, test_file_name = setup_fixture
    df = fpd.read_csv(test_file_name)
    setup_df = setup.copy() # setting items on copy

    setup_df['const_int'] = 20
    df['const_int'] = 20
    assert(setup_df["const_int"].equals(df['const_int'].read_into_mem()))

    setup_df['const_str'] = "test_string"
    df['const_str'] = "test_string"
    assert(setup_df["const_str"].equals(df['const_str'].read_into_mem()))

    # setup_df['const_bool'] = True
    # df['const_bool'] = True
    # assert(setup_df["const_bool"].equals(df['const_bool'].read_into_mem()))


# def test_getter_columns(setup_fixture):
#     setup, test_file_name = setup_fixture
#     df = fpd.read_csv(test_file_name)
#     cols = ['A', 'B']
#     assert(setup[cols].equals(df[cols].read_into_mem()))


# def test_head(setup_fixture):
#     setup, test_file_name = setup_fixture
#     df = fpd.read_csv(test_file_name)
#     assert(setup.head().equals(df.head().read_into_mem()))
#     assert(setup.head(2).equals(df.head(2).read_into_mem()))


# def test_getter_mask(setup_fixture):
#     setup, test_file_name = setup_fixture
#     df = fpd.read_csv(test_file_name)
#     pd_mask = setup['A'] == 2
#     fpd_mask = df['A'] == 2
#     assert(setup[pd_mask].equals(df[fpd_mask].read_into_mem()))