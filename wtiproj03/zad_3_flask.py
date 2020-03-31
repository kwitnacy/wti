from flask import Flask, request
import statistics as stat
import pandas as pd
import numpy as np
import json
import math
import sys


user_rated_movies = pd.read_csv('/home/kwitnoncy/Documents/politechnika/wti/wtiproj03/data/joined.dat', sep='\t')
head = user_rated_movies.columns.values
user_rated_movies[np.isnan] = 0

dummy_rating = {}
only_dummy = False

app = Flask(__name__)


@app.route('/rating', methods=['POST'])
def rating():
    global user_rated_movies
    if type(request.json) == str:
        data = json.loads(request.json)
    else:
        data = request.json
    try:
        user_rated_movies = user_rated_movies.append(pd.json_normalize(data))
        user_rated_movies[np.isnan] = 0
        # print(user_rated_movies.tail(2))
    except: 
        return json.dumps({"method": "POST", "status": "Error", "message": sys.exc_info()[0]})
    return json.dumps({"method": "POST", "status": "OK"}), 201


@app.route('/ratings', methods=['GET', 'DELETE'])
def ratings():
    global user_rated_movies
    if request.method == 'GET':
        try:
            return json.dumps({x: int(y) if not math.isnan(y) else 0 for x, y in zip(head, user_rated_movies.tail(1).to_numpy()[0])}), 200
        except IndexError:
            return json.dumps({x: 0 for x in head})

    elif request.method == 'DELETE':
        try:
            user_rated_movies = user_rated_movies.iloc[0:0]
        except:
            return json.dumps({"method": "DELETE", "status": "Error", "message": sys.exc_info()[0]}), 200
        # print(user_rated_movies)
        return json.dumps({"method": "DELETE", "status": "OK"})


@app.route('/avg-genre-ratings/all-users', methods=['GET'])
def get_avg_all_users():
    global user_rated_movies
    try:
        return json.dumps({x:y for x, y in zip(head[9:], [stat.mean(user_rated_movies[genre].to_list()) for genre in head[9:]])}), 200
    except:
        return json.dumps({"status": "Database is empty"})


@app.route('/avg-genre-ratings/<user_ID>', methods=['GET'])
def get_avg_user(user_ID: int):
    global user_rated_movies

    query = 'userID == ' + user_ID
    try:
        return json.dumps({x:y for x, y in zip(head[9:], [stat.mean(user_rated_movies.query(query)[genre].to_list()) for genre in head[9:]])}), 200
    except:
        return json.dumps({"status": "Database is empty"})


@app.route('/test')
def test():
    return 'test'


if __name__ == '__main__':
    app.run(threaded=False, processes=10)

