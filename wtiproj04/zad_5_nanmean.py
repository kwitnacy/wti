import pandas as pd
import numpy as np
from zad_1_join import my_join

# head = my_join()
head = ['Action', 'Adventure', 'Animation', 'Children', 'Comedy', 'Crime', 'Documentary', 'Drama', 'Fantasy', 'Film-Noir', 'Horror', 'IMAX', 'Musical', 'Mystery', 'Romance', 'Sci-Fi', 'Short', 'Thriller', 'War', 'Western']
df_main = pd.read_csv('/home/kwitnoncy/Documents/politechnika/wti/wtiproj03/data/joined.dat', sep='\t')

copy = df_main.to_numpy()

avg = [np.nanmean([(row[2] * row[genre + 9]) if row[genre + 9] != 0 else np.nan for row in copy]) for genre in range(len(head))]
avg_rating = pd.DataFrame([avg], columns=head)

avg_rating.to_csv('/home/kwitnoncy/Documents/politechnika/wti/wtiproj04/data/avg_rating.dat', index=False, sep='\t')



