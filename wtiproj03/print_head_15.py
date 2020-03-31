import pandas as pd

df = pd.read_csv('/home/kwitnoncy/Documents/politechnika/wti/wtiproj03/data/joined.dat', sep='\t')

print(df.query('userID == 78').head(15))
