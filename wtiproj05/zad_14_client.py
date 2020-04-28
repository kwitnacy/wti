import pandas as pd
import requests
import time
import json

port = 5000
url  = 'http://127.0.0.1:' + str(port)


times_flask = {
    'GET': [],
    'POST': [],
    'DELETE': []
}

times_cherry = {
    'GET': [],
    'POST': [],
    'DELETE': []
}

data1 = {
    "userID": 78,
    "movieID": 903,
    "rating": 4,
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


data2 = {
    "userID": 103301,
    "movieID": 903,
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


for _ in range(10):
    start = time.time()
    r = requests.get(url + '/ratings')
    times_flask['GET'].append(time.time() - start)
    time.sleep(0.01)


    start = time.time()
    r = requests.get(url + '/avg-genre-ratings/all-users')
    times_flask['GET'].append(time.time() - start)
    time.sleep(0.01)


    start = time.time()
    r = requests.get(url + '/avg-genre-ratings/78')
    times_flask['GET'].append(time.time() - start)
    time.sleep(0.01)


    start = time.time()
    r = requests.post(url + '/rating', json=json.dumps(data1))
    times_flask['GET'].append(time.time() - start)
    time.sleep(0.01)


    start = time.time()
    r = requests.get(url + '/ratings')
    times_flask['GET'].append(time.time() - start)
    time.sleep(0.01)


    start = time.time()
    r = requests.delete(url + '/ratings')
    times_flask['DELETE'].append(time.time() - start)
    time.sleep(0.01)


    start = time.time()
    r = requests.post(url + '/rating', json=json.dumps(data2))
    times_flask['POST'].append(time.time() - start)
    time.sleep(0.01)


    start = time.time()
    r = requests.get(url + '/avg-genre-ratings/all-users')
    times_flask['GET'].append(time.time() - start)
    time.sleep(0.01)


    start = time.time()
    r = requests.get(url + '/avg-genre-ratings/103301')
    times_flask['GET'].append(time.time() - start)
    time.sleep(0.01)


    start = time.time()
    r = requests.get(url + '/avg-genre-ratings/78')
    times_flask['GET'].append(time.time() - start)
    time.sleep(0.01)




port = 9898
url  = 'http://127.0.0.1:' + str(port)


for _ in range(10):
    start = time.time()
    r = requests.get(url + '/ratings')
    times_cherry['GET'].append(time.time() - start)
    time.sleep(0.01)


    start = time.time()
    r = requests.get(url + '/avg-genre-ratings/all-users')
    times_cherry['GET'].append(time.time() - start)
    time.sleep(0.01)


    start = time.time()
    r = requests.get(url + '/avg-genre-ratings/78')
    times_cherry['GET'].append(time.time() - start)
    time.sleep(0.01)


    start = time.time()
    r = requests.post(url + '/rating', json=json.dumps(data1))
    times_cherry['GET'].append(time.time() - start)
    time.sleep(0.01)


    start = time.time()
    r = requests.get(url + '/ratings')
    times_cherry['GET'].append(time.time() - start)
    time.sleep(0.01)


    start = time.time()
    r = requests.delete(url + '/ratings')
    times_cherry['DELETE'].append(time.time() - start)
    time.sleep(0.01)


    start = time.time()
    r = requests.post(url + '/rating', json=json.dumps(data2))
    times_cherry['POST'].append(time.time() - start)
    time.sleep(0.01)


    start = time.time()
    r = requests.get(url + '/avg-genre-ratings/all-users')
    times_cherry['GET'].append(time.time() - start)
    time.sleep(0.01)


    start = time.time()
    r = requests.get(url + '/avg-genre-ratings/103301')
    times_cherry['GET'].append(time.time() - start)
    time.sleep(0.01)


    start = time.time()
    r = requests.get(url + '/avg-genre-ratings/78')
    times_cherry['GET'].append(time.time() - start)
    time.sleep(0.01)



print('flask_get = ', times_flask['GET'])
print('flask_post = ', times_flask['POST'])
print('flask_delete = ', times_flask['DELETE'])
print('\n\n')
print('cherry_get = ', times_cherry['GET'])
print('cherry_post = ', times_cherry['POST'])
print('cherry_delete = ', times_cherry['DELETE'])

# pd.DataFrame.from_dict(times_cherry).to_csv('latency_cherry.csv')
# pd.DataFrame.from_dict(times_flask).to_csv('latency_flask.csv')