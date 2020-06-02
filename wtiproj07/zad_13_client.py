import requests
import re

url = 'http://127.0.0.1:5000'


def print_meta(r, body: dict = {}):
    print('url:         ', r.url)
    print('status code: ', r.status_code)
    print('headers:     ', r.headers)
    print('text:        ', r.text)
    
    if body:
        print('body:    ', body)

    print('-'*100)


def send_get(mess: str, endpoint: str):
    print(mess)
    u = url + endpoint
    r = requests.get(u)
    print_meta(r)


def send_post(mess: str, endpoint: str, data: dict = {}):
    print(mess)
    u = url + endpoint
    r = requests.post(u, json=data)
    print_meta(r)


def send_put(mess: str, endpoint: str, data: dict = {}):
    print(mess)
    u = url + endpoint
    if data:
        r = requests.put(u, data=data, headers={"Content-Type": "application/json"})
        print_meta(r, data)
    else:
        r = requests.put(u)
        print_meta(r)


def send_delete(mess: str, endpoint: str):
    print(mess)
    u = url + endpoint
    r = requests.delete(u)
    print_meta(r)
    

send_get('Document for user (ID = 75)', '/user/document/75')
send_get('Document for wrong user', '/user/document/0')
send_get('Document for movie (ID = 3)', '/movie/document/3')

send_get('Prediction for user (ID = 75)', '/user/prediction/75')
send_get('Prediction for movie (ID = 3)', '/movie/prediction/3')

send_put('New movie (ID = 80000), likes 75', '/movie/document/80000', '[75]')
send_put('New movie (ID = 80001), likes 75', '/movie/document/80001', '[75]')
send_put('New movie (ID = 80002), likes 75', '/movie/document/80002', '[75]')

send_put('New User (ID = 90000), who likes movies 80000 and 80001', '/user/document/90000', '[80000, 80001]')
send_put('New User (ID = 90001), who likes movies 80002 and 80001', '/user/document/90001', '[80002, 80001]')

send_get('Document for user (ID = 90000)', '/user/document/90000')
send_get('Updated movie (ID = 80001)', '/movie/document/80001')


send_post(
    'Update user (ID = 90000) that he likes movies 80000, 80001, 80002, 3',
    '/user/mlp',
    [{'user_id': 90000, 'liked_movies': [80000, 80001, 80002, 3]}])

send_get('Get user (ID = 90000) document', '/user/document/90000')
send_get('Get updated movie (ID = 80001)', '/movie/document/80001')
send_get('Get updated movie (ID = 80002)', '/movie/document/80002')
send_get('Get updated movie (ID = 3)', '/movie/document/3')


send_delete('Remove movie (ID = 80000)', '/movie/document/80000')
send_delete('Remove movie (ID = 80001)', '/movie/document/80001')
send_delete('Remove movie (ID = 80002)', '/movie/document/80002')

send_delete('Remove user (ID = 90000)', '/user/document/90000')
send_delete('Remove user (ID = 90001)', '/user/document/90001')

# print(requests.get(url+'/user/document/75'))
