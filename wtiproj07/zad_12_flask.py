from flask import Flask, abort, request, jsonify
from zad_9_elactic_simple_client import ElasticClient


app = Flask(__name__)
es = ElasticClient()


@app.route('/user/document/<user_id>', methods=['GET'])
def get_user_by_id(user_id: int):
    try:
        user_id = int(user_id)
        index = request.args.get('index', default='users')
        result = es.get_movies_liked_by_user(user_id, index=index)
        return jsonify(result)
    except:
        abort(404)


@app.route('/movie/document/<movie_id>', methods=['GET'])
def get_movie_by_id(movie_id):
    try:
        movie_id = int(movie_id)
        index = request.args.get('index', default='movies')
        result = es.get_users_that_like_movie(movie_id, index=index)
        return jsonify(result)
    except:
        abort(404)


@app.route('/user/prediction/<user_id_pred>', methods=['GET'])
def get_user_pred_by_id(user_id_pred):
    try:
        user_id_pred = int(user_id_pred)
        result = es.get_predicion_based_user(user_id_pred)
        return jsonify(result)
    except:
        abort(404)


@app.route('/movie/prediction/<movie_id_pred>', methods=['GET'])
def get_movie_pred_by_id(movie_id_pred):
    try:
        movie_id_pred = int(movie_id_pred)
        result = es.get_predicion_based_movie(movie_id_pred)
        return jsonify(result)
    except:
        abort(404)


@app.route('/user/document/<user_id>', methods=['PUT'])
def add_user_document(user_id):
    try:
        user_id = int(user_id)
        movies_liked = request.json
        es.add_user_document(user_id, movies_liked)
        return "OK", 200
    except:
        abort(404)


@app.route('/movie/document/<movie_id>', methods=['PUT'])
def add_movie_document(movie_id):
    # try:
    movie_id = int(movie_id)
    users_liked = request.json
    es.add_movie_document(movie_id, users_liked)
    return "OK", 200
    # except:
    #     abort(404)

        
@app.route('/user/document/<user_id>', methods=['POST'])
def update_user_document(user_id):
    try:
        user_id = int(user_id)
        movies_liked = request.json
        es.update_user(user_id, movies_liked)
        return "OK", 200
    except:
        abort(404)

        
@app.route('/movie/document/<movie_id>', methods=['POST'])
def update_movie_document(movie_id):
    try:
        movie_id = int(movie_id)
        users_liked = request.json
        es.update_user(movie_id, users_liked)
        return "OK", 200
    except:
        abort(404)

        
@app.route('/user/document/<user_id>', methods=['DELETE'])
def delete_user_document(user_id):
    # try:
    user_id = int(user_id)
    es.delete_user(user_id)
    return "OK", 200
    # except:
    #     abort(404)

        
@app.route('/movie/document/<movie_id>', methods=['DELETE'])
def delete_movie_document(movie_id):
    #try:
    movie_id = int(movie_id)
    es.delete_movie(movie_id)
    return "OK", 200
    # except:
    #     abort(404)


@app.route('/movie/mlp', methods=['POST'])
def update_mlp_movies():
    body = request.json
    es.mlp_movie_update(body)
    return "OK", 200


@app.route('/user/mlp', methods=['POST'])
def update_mlp_users():
    body = request.json
    es.mlp_user_update(body)
    return "OK", 200


@app.route('/test')
def test():
    return 'test'
        

if __name__ == '__main__':
    es.index_documents()
    app.run()