import fatpanda as fpd
import pandas as pd
import sqlite3
import pytest

def test_csv():
    df = fpd.concat_csv(["csv1.csv", "csv1.csv"])
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

    filtered = df.loc[(df['A']==2), ["A", "D"]]
    print(filtered)
    print(filtered.get_sql())
    print()

    print(df.loc[3, ["A", "D"]])
    pddf = pd.read_csv("csv1.csv")
    df = fpd.read_csv("csv1.csv")
    print(pddf[7:2:-1])
    sliced = df[7:2:-1]
    print(sliced)
    print(sliced.get_sql())



if __name__ == "__main__":
    # pytest.main()
    pytest.main(["-v"])
    test_csv()



