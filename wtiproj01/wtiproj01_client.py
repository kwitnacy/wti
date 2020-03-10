import requests
import time
from redis import Redis
from rq import Queue

from mars import get_mars_photo as photo


def count_words_at_url(url):
    resp = requests.get(url)
    return len(resp.text.split())

q = Queue(connection=Redis(port=6381))

jobs = []

jobs.append(q.enqueue(photo, 1000))
jobs.append(q.enqueue(photo, 1001))
jobs.append(q.enqueue(photo, 1002))
jobs.append(q.enqueue(photo, 1003))
jobs.append(q.enqueue(photo, 1004))

print("Before sleep")
print("count: ", q.count)
for job in jobs:
    print(job.result)

time.sleep(10)

print("After sleep")
print("count: ", q.count)
for job in jobs:
    print(job.result)
