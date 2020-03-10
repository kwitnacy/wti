import requests
import time
from redis import Redis

redis = Redis(port=6381)

redis.lpush("lista", "wartosc1")
redis.lpush("lista", "wartosc2")
redis.lpush("lista", "wartosc3")
redis.lpush("lista", "wartosc4")
redis.lpush("lista", "wartosc5")
redis.lpush("lista", "wartosc6")

print("Ilosc elementow:", redis.llen("lista"))
print("Elementy na liscie od 2 do 4: ", redis.lrange("lista", 2, 4))

print("Pop na liscie: ", redis.lpop("lista"))


print("Ilosc elementow:", redis.llen("lista"))
print("Elementy na liscie od 2 do 4: ", redis.lrange("lista", 2, 4))


print("usuniecie calej listy: ", redis.flushdb())
