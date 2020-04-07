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


@app.route('/test')
def test():
    return 'test'


if __name__ == '__main__':
    app.run()
