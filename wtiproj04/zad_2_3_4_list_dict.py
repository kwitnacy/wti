import pandas as pd


df_main = pd.read_csv('/home/kwitnoncy/Documents/politechnika/wti/wtiproj03/data/joined.dat', sep='\t')

print(type(df_main))

print(df_main.size)
print(df_main.shape)

# from pd.DataFrame to List[dict]
df_main_list = df_main.to_dict('records')

print(type(df_main_list))

# from List[dict] to pd.DataFrame
df_copy = pd.DataFrame.from_records(df_main_list)

# check if df_main and df_copy are the same as they should
print(df_main.equals(df_copy))

