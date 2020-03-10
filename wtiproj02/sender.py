import requests
import time
import json
from redis import Redis

redis = Redis(port=6381)


for i in range(100):
    redis.rpush("lista", json.dumps({"klucz1":i, "klucz2":i**i, "wiadomosc":"w"*i}))
    time.sleep(0.1)

print("koniec wysylania")