import fatpanda as fpd
import sqlite3

def test_csv():
    df = fpd.concat_csv(["csv2.csv", "csv1.csv"])
    print(df.head())
    filtered = df[df['C']>5]


    df['P'] = df['B'] % df['C']
    df['K'] = df['P'] * 100 * 2
    print(df)

    filtered['P'] = (filtered['B'] + 1) * 20
    print(filtered.get_sql())
    print(filtered)

    P = df['P']
    print(P)
    print(type(P))
    print(df[['A', 'B']])

def test_sql():
    con = sqlite3.connect(fpd.SQLITE_NAME)
    df = fpd.read_sql_query("select * from csv1_csv_2_CONCAT", con)

    print(df)
    print(df[df['C']>5])

if __name__ == "__main__":
    test_csv()
    # test_sql()



