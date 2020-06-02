import zad_6_cassandra_driver
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from redis import Redis
from typing import List
import statistics as stat
import pandas as pd
import numpy as np
import json
import sys

import zad_6_cassandra_driver


class API():
	def __init__(self):
		self.genres = [
				'Action', 'Adventure', 'Animation', 'Children', 'Comedy', 'Crime', 'Documentary',
				'Drama', 'Fantasy', 'FilmNoir', 'Horror', 'IMAX', 'Musical', 'Mystery',
				'Romance', 'SciFi', 'Short', 'Thriller', 'War', 'Western'
				]
		self.head = [
				'userID', 'movieID', 'rating', 'date_day', 'date_month', 'date_year', 'date_hour', 'date_minute',
				'date_second', 'Action', 'Adventure', 'Animation', 'Children', 'Comedy', 'Crime', 'Documentary',
				'Drama', 'Fantasy', 'FilmNoir', 'Horror', 'IMAX', 'Musical', 'Mystery', 'Romance', 'SciFi',
				'Short', 'Thriller', 'War', 'Western'
				]
		self.data = pd.read_csv('/home/kwitnoncy/Documents/politechnika/wti/wtiproj05/data/joined.dat', sep='\t')

		self.keyspace = "user_ratings"
		self.table = "user_avg_rating"
		cluster = Cluster(['127.0.0.1'], port=9042)
		self.cassandra_session = cluster.connect()
		# zad_6_cassandra_driver.delete_table(self.cassandra_session, self.keyspace, self.table)
		zad_6_cassandra_driver.create_keyspace(self.cassandra_session, self.keyspace)
		self.cassandra_session.set_keyspace(self.keyspace)
		self.cassandra_session.row_factory = dict_factory
		zad_6_cassandra_driver.create_table(self.cassandra_session, self.keyspace, self.table)

		data_from_cassandra = zad_6_cassandra_driver.get_data_table(self.cassandra_session, self.keyspace, self.table)

		print('before: ', self.data.shape)
		for row in data_from_cassandra:
			print(row)
			self.data = self.data.append(pd.DataFrame(row, columns=self.head, index=[0]))
		
		print('after: ', self.data.shape)


		temp_avg = self.get_avg_rating_genre()
		self.avg_per_gerne = np.array(temp_avg[0])
		self.count_per_gerne = np.array(temp_avg[1])


	def add_new_review(self, review: dict = {}) -> str:
		if review:
			try:
				self.data = self.data.append(pd.json_normalize(review))
				#self.data[np.isnan] = 0
				self.update_avg(review)
				"""
				vals = list(review.values())
					
				s = ''
				for v in vals:
					s += str(v)
					s += '|'

				s = s[:-1]
				print('adding to redis: ', s)
				self.redis_conn.lpush('reviews', s)
				"""
				print(list(review.values()))

				zad_6_cassandra_driver.push_data_table(
					self.cassandra_session, 
					self.keyspace, 
					self.table,
					list(review.values()))

			except IndexError:
				return str(sys.exc_info()[0])

			return 'OK'

		return None


	def get_last(self) -> dict:
		try:
			print(self.data.tail(1))
			return json.dumps({x: int(y) for x, y in zip(self.head, self.data.tail(1).to_numpy()[0])}), 200
		except IndexError:
			return json.dumps({x: 0 for x in self.genres}), 200
		except ValueError:
			print(json.dumps({x: y for x, y in zip(self.head, self.data.tail(1).to_numpy()[0])}))
			return json.dumps({x: y for x, y in zip(self.head, self.data.tail(1).to_numpy()[0])}), 200


	def flush_data(self) -> str:
		try:
			self.data = self.data.iloc[0:0]
			self.update_avg(None)

			"""
			self.redis_conn.flushdb() # delete redis database
			"""

			# TODO
			# delete data form cassandra keep table
			zad_6_cassandra_driver.clear_table(self.cassandra_session, self.keyspace, self.table)
			
		except:
			return sys.exc_info()[0]
        
		return 'OK'

	
	def get_avg_genre_all_users(self) -> dict:
		try:
			return json.dumps({x:y for x, y in zip(self.genres, self.avg_per_gerne)}), 200
		except:
			return json.dumps({"status": "Database is empty"})

	
	def update_avg(self, review: dict = {}) -> None:
		if review:
			rating = review["rating"]
			genres = list(review.values())[9:]
			
			for i in range(len(genres)):
				if genres[i] == 1:
					self.avg_per_gerne[i] = (self.avg_per_gerne[i] * self.count_per_gerne[i] + rating) / (self.count_per_gerne[i]+1)

			self.count_per_gerne = self.count_per_gerne + np.array(genres)
		else:
			self.avg_per_gerne = [0.0 for i in range(len(self.genres))]
			self.count_per_gerne = [0 for i in range(len(self.genres))]
	

	def get_user_profile(self, user_ID) -> dict:
		profile = np.array(self.avg_per_gerne) - self.get_avg_rating_genre_user(user_ID=user_ID)
		
		ret = {x: y for x, y in zip(self.genres, profile)}
		ret['userID'] = user_ID

		return json.dumps(ret)
	
	def my_join(self) -> List[str]:
		df_main = pd.read_csv('/home/kwitnoncy/Documents/politechnika/wti/wtiproj05/data/user_ratedmovies.dat', sep='\t')
		df_genres = pd.read_csv('/home/kwitnoncy/Documents/politechnika/wti/wtiproj05/data/movie_genres.dat', sep='\t')
		df_genres['Dummy'] = 1

		df_pivoted = df_genres.pivot_table(index='movieID', columns='genre', values='Dummy')
		joined = df_main.join(df_pivoted, on='movieID')
		joined[np.isnan] = 0
		joined.to_csv('/home/kwitnoncy/Documents/politechnika/wti/wtiproj03/data/joined.dat', index=False, sep='\t')

		return list(df_pivoted.columns.values)

	def get_avg_rating_genre(self) -> (np.array, np.array):
		joined = self.data.to_numpy()

		avg = [np.nanmean([(row[2] * row[genre + 9]) if row[genre + 9] != 0 else np.nan for row in joined]) for genre in range(len(self.genres))]

		pd.DataFrame([avg], columns=self.genres).to_csv('/home/kwitnoncy/Documents/politechnika/wti/wtiproj04/data/avg_rating.dat', index=False, sep='\t')
		
		return np.array(avg), np.array([np.count_nonzero(col) for col in joined.T[9:]])

	def get_avg_rating_genre_user(self, user_ID: int = 0) -> np.array:
		query = 'userID == ' + str(user_ID)
		
		data = self.data.query(query).to_numpy()

		return np.array([np.nanmean([(row[2] * row[genre + 9]) if row[genre + 9] != 0 else np.nan for row in data]) for genre in range(len(self.genres))])

