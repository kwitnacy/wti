import requests
import time
import sys
import json
from redis import Redis

redis = Redis(port=6381)
sleep_time = float(sys.argv[1]) if len(sys.argv) == 2 else 0.25

time.sleep(1)
recv = []
while(1):
    lista = redis.lrange("lista", 0, -1)
    if lista:
        data  = json.loads(lista[0])
        print(data)
        recv.append(data["id"])
    else:
        print(recv)

    redis.ltrim("lista", 1, -1)

    time.sleep(sleep_time)

print('koniec odbierania')