import fatpanda as fpd
import sqlite3
import pytest

def test_csv():
    df = fpd.concat_csv(["csv2.csv", "csv1.csv"])
    print(df.head())
    filtered = df[df['A']==2]

    df['P'] = df['B'] % df['C']
    df['K'] = df['P'] * 100 * 2
    print(df)

    filtered['P'] = (filtered['B'] + 1) * 20
    print(filtered.get_sql())
    print(filtered)

    P = df['P']
    print(P[0])
    print(type(P))

    cols = set(['A', 'B', 'A'])
    print(df[cols].head())



if __name__ == "__main__":
    # pytest.main()
    pytest.main(["-v"])
    # test_csv()
    # test_sql()


