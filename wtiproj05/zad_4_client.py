import requests
import time
import json


data2 = {
    "userID": 13371337,
    "movieID": 13371337,
    "rating": 8,
    "date_day": 29,
    "date_month": 10,
    "date_year": 2006,
    "date_hour": 23,
    "date_minute": 17,
    "date_second": 16,
    "Action": 0,
    "Adventure": 0,
    "Animation": 0,
    "Children": 0,
    "Comedy": 0,
    "Crime": 0,
    "Documentary": 0,
    "Drama": 1,
    "Fantasy": 0,
    "Film-Noir": 0,
    "Horror": 0,
    "IMAX": 0,
    "Musical": 0,
    "Mystery": 1,
    "Romance": 1,
    "Sci-Fi": 0,
    "Short": 0,
    "Thriller": 1,
    "War": 0,
    "Western": 0
}


def print_meta(r):
    print('url:         ', r.url)
    print('status code: ', r.status_code)
    print('headers:     ', r.headers)
    print('text:        ', r.text)
    print('-'*100)


r = requests.post('http://127.0.0.1:5000/rating', json=json.dumps(data2))
print_meta(r)
time.sleep(0.01)


r = requests.get('http://127.0.0.1:5000/ratings')
print_meta(r)
time.sleep(0.01)
