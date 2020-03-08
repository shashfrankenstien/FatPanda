import pandas  as pd

import fatpanda as fpd
from tests import setup_fixture, assert_df_equal



def test_getter_column_name(setup_fixture):
    setup, df, _ = setup_fixture
    A = df["A"]
    assert(isinstance(A, fpd.core._Series))
    assert_df_equal(setup['A'], A)


def test_getter_columns(setup_fixture):
    setup, df, _ = setup_fixture
    cols = ['A', 'B']
    assert_df_equal(setup[cols], df[cols])


def test_getter_virtual(setup_fixture):
    setup, _, test_file_name = setup_fixture
    setup_df = setup.copy() # setting items on a copy
    df = fpd.read_csv(test_file_name)

    setup_df['K'] = 20
    df['K'] = 20
    assert(isinstance(df['K'], fpd.core._VirtualSeries))
    assert_df_equal(setup_df["K"], df['K'])


def test_head(setup_fixture):
    setup, df, _ = setup_fixture
    assert_df_equal(setup.head(), df.head())
    assert_df_equal(setup.head(2), df.head(2))
    assert_df_equal(setup.head(1), df.head(1))
    # Series head test
    assert_df_equal(setup['A'].head(), df['A'].head())
    assert_df_equal(setup['D'].head(2), df['D'].head(2))


def test_mask(setup_fixture):
    setup, df, _ = setup_fixture

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



def test_mask2(setup_fixture):
    setup, df, _ = setup_fixture

    pd_mask = (setup['D']=='man') & (setup['A']==2)
    fpd_mask = (df['D']=='man') & (df['A']==2)
    assert_df_equal(setup[pd_mask], df[fpd_mask])

    pd_mask = (setup['D']=='man') | (setup['A']==2)
    fpd_mask = (df['D']=='man') | (df['A']==2)
    assert_df_equal(setup[pd_mask], df[fpd_mask])

    assert_df_equal(setup[~pd_mask], df[~fpd_mask])


def test_mask_virtual(setup_fixture):
    setup, _, test_file_name = setup_fixture
    setup_df = setup.copy() # setting items on a copy
    df = fpd.read_csv(test_file_name)

    setup_df['Virtual'] = (setup_df['B'] + 10) * setup_df['C']
    df['Virtual'] = (df['B'] + 10) * df['C']

    pd_mask = (setup_df['Virtual']>45) & (setup_df['Virtual']<979)
    fpd_mask = (df['Virtual']>45) & (df['Virtual']<979)
    assert_df_equal(setup_df[pd_mask], df[fpd_mask])



def test_mask_slice(setup_fixture):
    setup, df, _ = setup_fixture

    assert_df_equal(setup[5:], df[5:])
    assert_df_equal(setup[2:6], df[2:6])
    assert_df_equal(setup[:7:3], df[:7:3])
    assert_df_equal(setup[2:7:2], df[2:7:2])
    assert_df_equal(setup[::5], df[::5])
    assert_df_equal(setup[::], df[::])
    assert_df_equal(setup[::-1], df[::-1])
    assert_df_equal(setup[7:2:-1], df[7:2:-1])
    print(setup[::-2], df[::-2])



def test_loc_getter(setup_fixture):
    setup, df, _ = setup_fixture

    setup_row_mask = (setup['A']==2)
    df_row_mask = (df['A']==2)
    assert_df_equal(setup.loc[setup_row_mask], df.loc[df_row_mask])

    setup_row_mask = (setup['A']>=2) & (setup['D']=='man')
    df_row_mask = (df['A']>=2) & (df['D']=='man')
    assert_df_equal(setup.loc[setup_row_mask], df.loc[df_row_mask])
    assert_df_equal(setup.loc[setup_row_mask, ], df.loc[df_row_mask, ])

    assert_df_equal(setup.loc[3], df.loc[3])
    assert_df_equal(setup.loc[3,], df.loc[3,])

    assert(setup.loc[3, "A"]==df.loc[3, "A"])
    assert_df_equal(setup.loc[3, ["A"]], df.loc[3, ["A"]])
    assert_df_equal(setup.loc[3, ["A", "D"]], df.loc[3, ["A", "D"]])

    assert_df_equal(setup.loc[setup_row_mask, ["A", "D"]], df.loc[df_row_mask, ["A", "D"]])

    assert_df_equal(setup.loc[2:7:2,], df.loc[2:7:2,])
    assert_df_equal(setup.loc[::2, ["A", "D"]], df.loc[::2, ["A", "D"]])



