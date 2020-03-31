import pandas as pd

def my_join():
    df_main = pd.read_csv('/home/kwitnoncy/Documents/politechnika/wti/wtiproj03/data/user_ratedmovies.dat', sep='\t')
    df_genres = pd.read_csv('/home/kwitnoncy/Documents/politechnika/wti/wtiproj03/data/movie_genres.dat', sep='\t')
    df_genres['Dummy'] = 1

    df_pivoted = df_genres.pivot_table(index='movieID', columns='genre', values='Dummy')
    joined = df_main.join(df_pivoted, on='movieID')
    joined.to_csv('/home/kwitnoncy/Documents/politechnika/wti/wtiproj03/data/joined.dat', index=False, sep='\t')


if __name__ == "__main__":
    my_join()