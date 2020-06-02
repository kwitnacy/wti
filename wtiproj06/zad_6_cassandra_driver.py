from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from typing import List

def create_keyspace(session, keyspace):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS """+keyspace+"""
        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
    """)

def create_table(session, keyspace, table):
    session.execute("""
        CREATE TABLE IF NOT EXISTS """+ keyspace+"""."""+table+""" (
		movieID int,
        userID int ,
		rating float,
		date_day int,
		date_month int,
		date_year int,
		date_hour int,
		date_minute int,
		date_second int,
		Action int,
		Adventure int, 
        Animation int,
        Children int,
        Comedy int,
        Crime int,
        Documentary int,
        Drama int,
        Fantasy int,
        FilmNoir int,
        Horror int,
        IMAX int,
        Musical int,
        Mystery int,
        Romance int,
        SciFi int,
        Short int,
        Thriller int,
        War int,
        Western int,
        PRIMARY KEY(userID)
        )"""
    )

def push_data_table(session, keyspace, table, data):
    print('adding: ', data)
    session.execute(
        """
        INSERT INTO """+keyspace+"""."""+table+""" (movieID, userID, rating, date_day, date_month, date_year, date_hour, date_minute, date_second, Action, Adventure, 
        Animation, Children, Comedy, Crime, Documentary, Drama, Fantasy, FilmNoir, Horror, IMAX, Musical, Mystery, Romance, SciFi, Short, Thriller, War, Western)
        VALUES (%(movieID)s, %(userID)s, %(rating)s, %(date_day)s, %(date_month)s, %(date_year)s, %(date_hour)s, %(date_minute)s, %(date_second)s, %(Action)s, %(Adventure)s, 
        %(Animation)s, %(Children)s, %(Comedy)s, %(Crime)s, %(Documentary)s, %(Drama)s, %(Fantasy)s, %(FilmNoir)s, %(Horror)s, %(IMAX)s, %(Musical)s, %(Mystery)s, %(Romance)s, 
        %(SciFi)s, %(Short)s, %(Thriller)s, %(War)s, %(Western)s)
        """,
            {
                'userID': data[0],
                'movieID': data[1],
                'rating': data[2],
                'date_day': data[3],
                'date_month': data[4],
                'date_year': data[5],
                'date_hour': data[6],
                'date_minute': data[7],
                'date_second': data[8],
                'Action': data[9],
                'Adventure': data[10],
                'Animation': data[11],
                'Children': data[12],
                'Comedy': data[13],
                'Crime': data[14],
                'Documentary': data[15],
                'Drama': data[16],
                'Fantasy': data[17],
                'FilmNoir': data[18],
                'Horror': data[19],
                'IMAX': data[20],
                'Musical': data[21],
                'Mystery': data[22],
                'Romance': data[23],
                'SciFi': data[24],
                'Short': data[25],
                'Thriller': data[26],
                'War': data[27],
                'Western': data[28]
            }
        )

def get_data_table(session, keyspace, table):
    rows = session.execute("SELECT * FROM "+keyspace+"."+table+";")
    
    return rows

def clear_table(session, keyspace, table):
    session.execute("TRUNCATE "+keyspace+"."+table+";")

def delete_table(session, keyspace, table):
    session.execute("DROP TABLE "+keyspace+"."+table+";")


