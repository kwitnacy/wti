import requests
import time
import json
import pandas as pd
import csv
import sys
from redis import Redis

redis = Redis(port=6381)
sender_id = sys.argv[1] if len(sys.argv) >= 2 else None
sleep_time = float(sys.argv[2]) if len(sys.argv) == 3 else 0.25

# 4.1 beg
df  = pd.read_csv('/home/kwitnoncy/Documents/politechnika/wti/wtiproj02/data/user_ratedmovies.dat', sep='\t')
col_names = df.columns.values[0].split(' ')
while '' in col_names:
    col_names.remove('')
# 4.1 end

# # 3 for
# for i in range(100):
#     redis.rpush("lista", json.dumps({"klucz1":i, "klucz2":i**i, "wiadomosc":"w"*i}))
#     time.sleep(0.1)

# 4 for
reviews_count = 250
counter = 0
for row, _ in df.iterrows():
    obj = {x: y for x, y in zip(col_names, row)}
    obj['id'] = counter
    obj['sender_id'] = sender_id

    redis.rpush("lista", json.dumps(obj))
    
    print('send: ', obj)

    counter += 1
    if counter > reviews_count:
        break
    time.sleep(sleep_time)

print("koniec wysylania")