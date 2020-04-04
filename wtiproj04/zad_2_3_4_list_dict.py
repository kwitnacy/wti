import pandas as pd


df_main = pd.read_csv('/home/kwitnoncy/Documents/politechnika/wti/wtiproj03/data/joined.dat', sep='\t')


# from pd.DataFrame to List[dict]
# Zadanie 2
# Zamiana z pd.DataFrame do list[dict]
df_main_list = df_main.to_dict('records')


# Sprawdzenie typu zmiennej
print(type(df_main_list))


# from List[dict] to pd.DataFrame
# Zadanie 3
# Zamiania z  list[dict] do pd.DataFrame
df_copy = pd.DataFrame.from_records(df_main_list)


# Sprawdzenie typu zmiennej
print(type(df_copy))


# check if df_main and df_copy are the same as they should
# Zadanie 4
# Sprawdzenie czy dane są takie same, czyli że nie doszło do utraty danych.
print(df_main.equals(df_copy))
