import redis

r = redis.Redis(port=6381)
if r.flushdb():
    print("cacy")
else:
    print("blad")

