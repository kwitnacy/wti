import pandas as pd
import numpy as np
from typing import List


def my_join() -> List[str]:
    df_main = pd.read_csv('/home/kwitnoncy/Documents/politechnika/wti/wtiproj03/data/user_ratedmovies.dat', sep='\t')
    df_genres = pd.read_csv('/home/kwitnoncy/Documents/politechnika/wti/wtiproj03/data/movie_genres.dat', sep='\t')
    df_genres['Dummy'] = 1

    df_pivoted = df_genres.pivot_table(index='movieID', columns='genre', values='Dummy')
    joined = df_main.join(df_pivoted, on='movieID')
    joined[np.isnan] = 0
    joined.to_csv('/home/kwitnoncy/Documents/politechnika/wti/wtiproj03/data/joined.dat', index=False, sep='\t')

    return list(df_pivoted.columns.values)


def my_to_dict() -> List[dict]:
    joined = pd.read_csv('/home/kwitnoncy/Documents/politechnika/wti/wtiproj03/data/joined.dat', sep='\t')

    return joined.to_dict('records')


def my_from_dict_to_dataframe(l: List[dict] = []) -> pd.DataFrame:
    return pd.DataFrame.from_records(l)


def get_avg_rating_genre() -> np.array:
    # head = my_join()
    head = ['Action', 'Adventure', 'Animation', 'Children', 'Comedy', 'Crime', 'Documentary', 'Drama', 'Fantasy', 'Film-Noir', 'Horror', 'IMAX', 'Musical', 'Mystery', 'Romance', 'Sci-Fi', 'Short', 'Thriller', 'War', 'Western']

    joined = pd.read_csv('/home/kwitnoncy/Documents/politechnika/wti/wtiproj03/data/joined.dat', sep='\t')
    joined = joined.to_numpy()

    avg = [np.nanmean([(row[2] * row[genre + 9]) if row[genre + 9] != 0 else np.nan for row in joined]) for genre in range(len(head))]

    pd.DataFrame([avg], columns=head).to_csv('/home/kwitnoncy/Documents/politechnika/wti/wtiproj04/data/avg_rating.dat', index=False, sep='\t')

    return np.array(avg)


def get_avg_rating_genre_user(user_ID: int = 0) -> np.array:
    query = 'userID == ' + str(user_ID)
    # head = my_join()
    head = ['Action', 'Adventure', 'Animation', 'Children', 'Comedy', 'Crime', 'Documentary', 'Drama', 'Fantasy', 'Film-Noir', 'Horror', 'IMAX', 'Musical', 'Mystery', 'Romance', 'Sci-Fi', 'Short', 'Thriller', 'War', 'Western']

    joined = pd.read_csv('/home/kwitnoncy/Documents/politechnika/wti/wtiproj03/data/joined.dat', sep='\t')    
    joined = joined.query(query).to_numpy()

    return np.array([np.nanmean([(row[2] * row[genre + 9]) if row[genre + 9] != 0 else np.nan for row in joined]) for genre in range(len(head))])


def get_user_profile(user_ID: int = 0) -> np.array:
    avg = get_avg_rating_genre()
    user_avg = get_avg_rating_genre_user(user_ID)
    
    return np.nan_to_num(avg - user_avg)
