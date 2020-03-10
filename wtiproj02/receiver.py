import requests
import time
from redis import Redis

redis = Redis(port=6381)

time.sleep(1)

while(1):
    lista = redis.lrange("lista", 0, -1)

    print(lista[0])

    redis.ltrim("lista", 1, -1)

    time.sleep(0.2)