import fatpanda as fpd
import sqlite3
import pytest

def test_csv():
    df = fpd.concat_csv(["csv2.csv", "csv1.csv"])
    print(df)

    filtered = df[df['A']==2]
    filtered['const_int'] = 20
    filtered['const_bool'] = True
    filtered['const_str'] = "PPPooo"
    filtered['P'] = (filtered['B'] + 1) * filtered['const_int']
    print(filtered)
    print(filtered.get_sql())
    print()

    P = filtered['P']
    print(P[0])
    print(type(P))
    print()

    cols = set(['A', 'B', 'A'])
    print(df[cols].head())



if __name__ == "__main__":
    # pytest.main()
    pytest.main(["-v"])
    test_csv()


