import pandas as pd

df_main = pd.read_csv('/home/kwitnoncy/Documents/politechnika/wti/wtiproj03/data/user_ratedmovies.dat', sep='\t')
df_genres = pd.read_csv('/home/kwitnoncy/Documents/politechnika/wti/wtiproj03/data/movie_genres.dat', sep='\t')

df_genres['Dummy'] = 1
print(df_genres.head(10))

df_pivoted = df_genres.pivot_table(index='movieID', columns='genre', values='Dummy')

print(df_pivoted.head(10))
print(df_pivoted.columns.values)

joined = df_main.join(df_pivoted, on='movieID')

print(joined.head(10))

joined.to_csv('/home/kwitnoncy/Documents/politechnika/wti/wtiproj03/data/joined.dat', index=False, sep='\t')

print('opened file:')
opened = pd.read_csv('/home/kwitnoncy/Documents/politechnika/wti/wtiproj03/data/joined.dat', sep='\t')
print(opened.head(10))
