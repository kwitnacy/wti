import pandas as pd
import numpy as np
from typing import List


def my_join() -> List[str]:
    df_main = pd.read_csv('/home/kwitnoncy/Documents/politechnika/wti/wtiproj05/data/user_ratedmovies.dat', sep='\t')
    df_genres = pd.read_csv('/home/kwitnoncy/Documents/politechnika/wti/wtiproj05/data/movie_genres.dat', sep='\t')
    df_genres['Dummy'] = 1

    df_pivoted = df_genres.pivot_table(index='movieID', columns='genre', values='Dummy')
    joined = df_main.join(df_pivoted, on='movieID')
    joined[np.isnan] = 0
    joined.to_csv('/home/kwitnoncy/Documents/politechnika/wti/wtiproj03/data/joined.dat', index=False, sep='\t')

    return list(df_pivoted.columns.values)


def get_avg_rating_genre(data: pd.DataFrame = None) -> (np.array, np.array):
    # head = my_join()
    head = ['Action', 'Adventure', 'Animation', 'Children', 'Comedy', 'Crime', 'Documentary', 'Drama', 'Fantasy', 'Film-Noir', 'Horror', 'IMAX', 'Musical', 'Mystery', 'Romance', 'Sci-Fi', 'Short', 'Thriller', 'War', 'Western']

    joined = pd.DataFrame(data)
    joined = joined.to_numpy()

    avg = [np.nanmean([(row[2] * row[genre + 9]) if row[genre + 9] != 0 else np.nan for row in joined]) for genre in range(len(head))]

    pd.DataFrame([avg], columns=head).to_csv('/home/kwitnoncy/Documents/politechnika/wti/wtiproj04/data/avg_rating.dat', index=False, sep='\t')
    
    return np.array(avg), np.array([np.count_nonzero(col) for col in joined.T[9:]])


def get_avg_rating_genre_user(data: pd.DataFrame = None, user_ID: int = 0) -> np.array:
    query = 'userID == ' + str(user_ID)
    head = ['Action', 'Adventure', 'Animation', 'Children', 'Comedy', 'Crime', 'Documentary', 'Drama', 'Fantasy', 'Film-Noir', 'Horror', 'IMAX', 'Musical', 'Mystery', 'Romance', 'Sci-Fi', 'Short', 'Thriller', 'War', 'Western']
   
    data = data.query(query).to_numpy()

    return np.array([np.nanmean([(row[2] * row[genre + 9]) if row[genre + 9] != 0 else np.nan for row in data]) for genre in range(len(head))])


def get_user_profile(user_ID: int = 0) -> np.array:
    avg = get_avg_rating_genre()
    user_avg = get_avg_rating_genre_user(user_ID)
    
    return np.nan_to_num(avg - user_avg)
