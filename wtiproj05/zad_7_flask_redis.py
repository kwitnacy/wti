from flask import Flask, request
import statistics as stat
import pandas as pd
import numpy as np
import json
import math
import sys

from API_logic import API

api = API()


dummy_rating = {}
only_dummy = False

app = Flask(__name__)


@app.route('/rating', methods=['POST'])
def rating():
    if type(request.json) == str:
        data = json.loads(request.json)
    else:
        data = request.json
    
    mess = api.add_new_review(data)

    if mess != 'OK':
        return json.dumps({"method": "POST", "status": "Error", "message": mess}), 200
    else:
        return json.dumps({"method": "POST", "status": "OK"}), 201


@app.route('/ratings', methods=['GET', 'DELETE'])
def ratings():
    global user_rated_movies
    if request.method == 'GET':
        return api.get_last()

    elif request.method == 'DELETE':
        mess = api.flush_data()
        if mess != 'OK':
            return json.dumps({"method": "DELETE", "status": "Error", "message": mess}), 200
        else:
            return json.dumps({"method": "DELETE", "status": mess}), 200


@app.route('/avg-genre-ratings/all-users', methods=['GET'])
def get_avg_all_users():
    return api.get_avg_genre_all_users()


@app.route('/avg-genre-ratings/<user_ID>', methods=['GET'])
def get_avg_user(user_ID: int):
    return api.get_user_profile(user_ID), 200


@app.route('/test')
def test():
    return 'test'


if __name__ == '__main__':
    app.run(threaded=True)
