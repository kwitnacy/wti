import pandas as pd
import numpy as np
from typing import List


user_ID = 78
query = 'userID == ' + str(user_ID)
# head = my_join()
head = ['Action', 'Adventure', 'Animation', 'Children', 'Comedy', 'Crime', 'Documentary', 'Drama', 'Fantasy', 'Film-Noir', 'Horror', 'IMAX', 'Musical', 'Mystery', 'Romance', 'Sci-Fi', 'Short', 'Thriller', 'War', 'Western']

joined = pd.read_csv('/home/kwitnoncy/Documents/politechnika/wti/wtiproj03/data/joined.dat', sep='\t')    
joined = joined.query(query).to_numpy()

print([np.nanmean([(row[2] * row[genre + 9]) if row[genre + 9] != 0 else np.nan for row in joined]) for genre in range(len(head))])

