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


if __name__ == "__main__":
    my_join()