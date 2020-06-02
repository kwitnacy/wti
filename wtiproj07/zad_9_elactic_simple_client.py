import pandas as pd
import numpy as np
import random
from elasticsearch import Elasticsearch, helpers
from typing import List


class  ElasticClient:
	def __init__(self, address='localhost:10000'):
		self.es = Elasticsearch(address)


	def index_documents(self):
		df = pd.read_csv('data/user_ratedmovies.dat', delimiter='\t').loc[:, ['userID', 'movieID', 'rating']]
		means = df.groupby(['userID'], as_index=False, sort=False)\
			.mean()\
			.loc[:, ['userID', 'rating']]\
			.rename(columns={'rating': 'ratingMean'})
		
		df = pd.merge(df, means, on='userID', how='left', sort=False)
		df['ratingNormal'] = df['rating'] - df['ratingMean']

		ratings = df.loc[:, ['userID', 'movieID', 'ratingNormal']]\
			.rename(columns={'ratingNormal': 'rating'})\
			.pivot_table(index='userID', columns='movieID', values='rating')\
			.fillna(0)
	
		print('Indexing users...')
		index_users = [
			{
				'_index': 'users', 
				'_type': 'user', 
				'_id': index, 
				'_source':
					{
						'ratings': row[row > 0].sort_values(ascending=False).index.values.tolist()
					}
			} for index, row in ratings.iterrows()
		]
		helpers.bulk(self.es, index_users)
		print('DONE')	

		print('Indexing movies...')
		index_movies = [
			{
				'_index': 'movies', 
				'_type': 'movie', 
				'_id': column, 
				'_source':
					{
						'whoRated': ratings[column][ratings[column] > 0]\
							.sort_values(ascending=False)\
							.index\
							.values\
							.tolist()
					}
			} for column in ratings
		]
		helpers.bulk(self.es, index_movies)
		print('DONE')


	def get_movies_liked_by_user(self, user_id, index='users'):
		user_id = int(user_id)
		return self.es.get(index=index, doc_type="user" , id=user_id)["_source"]
	

	def get_users_that_like_movie(self, movie_id, index= 'movies' ):
		movie_id = int(movie_id)
		return self.es.get(index=index, doc_type="movie", id=movie_id)["_source"]


	def get_predicion_based_user(self, user_id: int):
		movies_rated = self.es.search(index='users', body={
			"query":{
				"term":{
					"_id": user_id
				}
			}
		})['hits']['hits'][0]['_source']['ratings']

		friend_user = self.es.search(index='users', body={
			"query":{
				"terms":{
					"ratings": movies_rated
				}
			}
		})['hits']['hits']

		result = set()

		for ratings in friend_user:
			if ratings['_id'] != user_id:
				ratings = ratings['_source']['ratings']
				for rating in ratings:
					if rating not in movies_rated:
						result.add(rating)

		return list(result)


	def get_predicion_based_movie(self, movie_id: int) -> List[int]:
		movie_id = int(movie_id)
		users_rated_movie = self.es.search(
			index='movies',
			body={
				"query":{
					"term":{
						"_id": movie_id
					}
				}
			}
		)['hits']['hits'][0]['_source']['whoRated']

		movies_rated_by_friend = self.es.search(
			index='movies',
			body={
				"query":{
					"terms":{
						"whoRated": users_rated_movie
					}
				}
			}
		)['hits']['hits']

		result = set()
		for ratings in movies_rated_by_friend:
			if ratings['_id'] != movie_id:
				ratings = ratings['_source']['whoRated']
				for rating in ratings:
					if rating not in users_rated_movie:
						result.add(rating)
		
		return list(result)


	def add_user_document(self, user_id: int, movies: List[int]):
		movies = list(set(movies))
		movies_to_update = [
			self.es.get(index='movies', id=movie_id, doc_type='movie') for movie_id in movies
		]
		
		if len(movies_to_update) != len(movies):
			raise Exception('wrong movies')

		for movie_doc in movies_to_update:
			users_liked = movie_doc['_source']['whoRated']
			users_liked.append(user_id)
			users_liked = list(set(users_liked))
			self.es.update(
				index='movies',
				id=movie_doc['_id'],
				doc_type='movie',
				body={
					"doc":{
						"whoRated": users_liked
					}
				}
			)

		self.es.create(
			index='users',
			id=user_id,
			body={
				"ratings": movies
			},
			doc_type='user'
		)
	

	def add_movie_document(self, movie_id: int, users: List[int]):
		if users:
			users = list(set(users))
		else:
			users = []

		users_to_update = [
			self.es.get(index='users', id=user_id, doc_type='user') for user_id in users
		]

		if len(users_to_update) != len(users):
			raise Exception('wrong users')

		for user_doc in users_to_update:
			movies_liked = user_doc['_source']['ratings']
			movies_liked.append(movie_id)
			movies_liked = list(set(movies_liked))
			self.es.update(
				index='users',
				id=user_doc['_id'],
				doc_type='user',
				body={
					"doc":{
						"ratings": movies_liked
					}
				}
			)

		self.es.create(
			index='movies',
			id=movie_id,
			body={
				"whoRated": users
			},
			doc_type='movie'
		)
	

	def update_user(self, user_id: int, movies: List[int]):
		user_id = int(user_id)
		liked_movies = list(set(movies))
		liked_movies = [int(x) for x in liked_movies]
		users_to_update = self.es.get(index='users', id=user_id, doc_type='user')
		old_movies = users_to_update['_source']['ratings']

		movies_to_add = np.setdiff1d(liked_movies, old_movies)
		movies_to_remove = np.setdiff1d(old_movies, liked_movies)

		for movie_to_remove in movies_to_remove:
			movie_doc = self.es.get(index='movies', id=movie_to_remove, doc_type='movie')
			users_who_like = movie_doc['_source']['whoRated']
			users_who_like.remove(user_id)
			users_who_like = list(set(users_who_like))
			self.es.update(
				index='movies',
				id=movie_to_remove,
				doc_type='movie',
				body={
					"doc":{
						"whoRated": users_who_like
					}
				}
			)
		
		for movie_to_add in movies_to_add:
			movie_doc = self.es.get(index='movies', id=movie_to_add, doc_type='movie')
			users_who_like = movie_doc['_source']['whoRated']
			users_who_like.append(user_id)
			users_who_like = list(set(users_who_like))
			self.es.update(
				index='movies',
				id=movie_to_add,
				doc_type='movie',
				body={
					"doc":{
						"whoRated": users_who_like
					}
				}
			)
		
		self.es.update(
			index='users',
			id=user_id,
			doc_type='user',
			body={
				"doc":{
					"ratings": liked_movies
				}
			}
		)
	

	def update_movie(self, movie_id: int, users: List[int]):
		movie_id = int(movie_id)
		users_liked_movies = list(set(users))
		users_liked_movies = [int(x) for x in users_liked_movies]
		movies_to_update = self.es.get(index='movies', id=movie_id, doc_type='movie')
		old_users = movies_to_update['_source']['whoRated']

		users_to_add = np.setdiff1d(users_liked_movies, old_users)
		users_to_remove = np.setdiff1d(old_users, users_liked_movies)

		for user_to_remove in users_to_remove:
			user_doc = self.es.get(index='users', id=user_to_remove, doc_type='user')
			movies_liked = user_doc['_source']['ratings']
			movies_liked.remove(movie_id)
			movies_liked = list(set(movies_liked))
			self.es.update(
				index='users',
				id=user_to_remove,
				doc_type='user',
				body={
					"doc":{
						"ratings": users_who_like
					}
				}
			)
		
		for user_to_add in users_to_add:
			user_doc = self.es.get(index='movies', id=user_to_add, doc_type='movie')
			movies_liked = user_doc['_source']['whoRated']
			movies_liked.append(user_id)
			movies_liked = list(set(movies_liked))
			self.es.update(
				index='users',
				id=user_to_add,
				doc_type='user',
				body={
					"doc":{
						"ratings": movies_liked
					}
				}
			)
		
		self.es.update(
			index='movies',
			id=movie_id,
			doc_type='movie',
			body={
				"doc":{
					"whoRated": users_liked_movies
				}
			}
		)


	def delete_user(self, user_id: int):
		user_doc = self.es.get(
			index='users',
			id=user_id,
			doc_type='user'
		)['_source']['ratings']

		for movie_id_to_remove in user_doc:
			movie_doc = self.es.get(
				index='movies',
				id=movie_id_to_remove,
				doc_type='movie'
			)
			users_who_liked = movie_doc['_source']['whoRated']
			try:
				users_who_liked.remove(user_id)
			except:
				pass
			self.es.update(
				index='movies',
				id=movie_id_to_remove,
				doc_type='movie',
				body={
					"doc": {
						"whoRated": users_who_liked
					}
				}
			)
		
		self.es.delete(index='users', id=user_id, doc_type='user')


	def delete_movie(self, movie_id: int):
		movie_id = int(movie_id)
		movie_doc = self.es.get(
			index='movies',
			id=movie_id,
			doc_type='movie'
		)['_source']['whoRated']

		for user_id_to_remove in movie_doc:
			user_doc = self.es.get(
				index='users',
				id=user_id_to_remove,
				doc_type='user'
			)
			movies_liked = user_doc['_source']['ratings']
			try:
				movies_liked.remove(movie_id)
			except:
				pass
			self.es.update(
				index='users',
				id=user_id_to_remove,
				doc_type='user',
				body={
					"doc": {
						"ratings": movies_liked
					}
				}
			)
		
		self.es.delete(index='movies', id=movie_id, doc_type='movie')
	

	def mlp_user_update(self, data):
		for user in data:
			self.update_user(
				user['user_id'],
				user['liked_movies']
			)
	

	def mlp_movie_update(self, data):
		for movie in data:
			self.update_movie(
				movie['movie_id'],
				movie['users_who_liked']
			)


if __name__ == "__main__":
	ec = ElasticClient()
	ec.index_documents()
	# ------ Simple operations ------
	print()
	user_document = ec.get_movies_liked_by_user(75 )
	movie_id = np.random.choice(user_document['ratings'])
	movie_document = ec.get_users_that_like_movie(movie_id)
	random_user_id = np.random.choice(movie_document['whoRated'])
	random_user_document = ec.get_movies_liked_by_user(random_user_id)
	print('User 75 likes following movies:')
	print(user_document)
	print('Movie {} is liked by following users:'.format(movie_id))
	print(movie_document)
	print('Is user 75 among users in movie {} document?' .format(movie_id))
	print(movie_document['whoRated'].index(75) != -1)
	some_test_movie_ID = 1
	print("Some test movie ID: ", some_test_movie_ID)
	list_of_users_who_liked_movie_of_given_ID = ec.get_users_that_like_movie(some_test_movie_ID)["whoRated"]
	print("List of users who liked the test movie: ", *list_of_users_who_liked_movie_of_given_ID)
	index_of_random_user_who_liked_movie_of_given_ID = random.randint(0, len(list_of_users_who_liked_movie_of_given_ID))
	print("Index of random user who liked the test movie: ", index_of_random_user_who_liked_movie_of_given_ID)
	some_test_user_ID =	list_of_users_who_liked_movie_of_given_ID[index_of_random_user_who_liked_movie_of_given_ID]
	print("ID of random user who liked the test movie: ", some_test_user_ID)
	movies_liked_by_user_of_given_ID = ec.get_movies_liked_by_user(some_test_user_ID)["ratings"]
	print("IDs of movies liked by the random user who liked the test movie: ", *movies_liked_by_user_of_given_ID)
	if some_test_movie_ID in movies_liked_by_user_of_given_ID:
		print ("As expected, the test movie ID is among the IDs of movies liked by the random user who liked the test movie ;-)")

	print('--------------------------------------')
	user_prediction = ec.get_predicion_based_user(75)
	print('Prediction')
	print(user_prediction)
	
	
	print('--------------------------------------')
	print('add/update')
	ec.add_user_document(99999, [3, 4])
	print('new user created, who likes movies 3, 4')
	print('user 99999 document:')
	print(ec.get_movies_liked_by_user(99999))
	print('but he likes new movie with id 98765')
	ec.add_movie_document(98765, [99999])
	print('new movie document')
	print(ec.get_users_that_like_movie(98765))
	print('but user 99999 watched and rated movie 2010')
	ec.update_user(99999, [3, 4, 98765, 2010])
	print('and it is his new "wathced list"')
	print(ec.get_movies_liked_by_user(99999))

	ec.delete_movie(98765)
	ec.delete_user(99999)
	
	

