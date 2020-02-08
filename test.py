import fatpanda as fpd

df = fpd.read_csv("csv.csv")

print(df[df['C']>10])

df['P'] = df['B'] % df['C']

print(df)

print(df["P"])