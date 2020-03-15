import pandas as pd
import csv
import json

df  = pd.read_csv('/home/kwitnoncy/Documents/politechnika/wti/wtiproj02/data/user_ratedmovies.dat', sep='\t')

col_names = df.columns.values[0].split(' ')
while '' in col_names:
    col_names.remove('')

print(col_names)
counter = 0
for row, _ in df.iterrows():
    obj = {x: y for x, y in zip(col_names, row)}

    print(obj)

    counter += 1
    if counter > 10:
        break
