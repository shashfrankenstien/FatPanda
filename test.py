import fatpanda as fpd

df = fpd.concat_csv(["csv2.csv", "csv1.csv"])
print(df.head())
filtered = df[df['C']>5]


df['P'] = df['B'] % df['C']
df['K'] = df['P'] * 100 * 2
print(df)

filtered['P'] = (filtered['B'] + 1) * 20
print(filtered)
print(filtered.get_sql())

P = df['P']
print(P)
print(type(P))
