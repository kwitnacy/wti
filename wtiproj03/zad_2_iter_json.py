import pandas as pd
import numpy as np
import json

user_rated_movies = pd.read_csv('/home/kwitnoncy/Documents/politechnika/wti/wtiproj03/data/joined.dat', sep='\t')
user_rated_movies[np.isnan] = 0
iterator = user_rated_movies.iterrows()

stop = 5
for _, row in iterator:
    print(json.dumps(dict(row)))
    stop -= 1
    if stop == 0:
        break

# head = user_rated_movies.columns.values
# last = user_rated_movies.tail(1).to_numpy()[0]

# print({x:y for x, y in zip (head, user_rated_movies.tail(1).to_numpy()[0])})