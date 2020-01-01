import os
import pandas as pd


os.chdir('../data')
list_df = list_files = os.listdir()

df = pd.DataFrame()
for df_name in list_files:
    print(df_name)
    df = pd.concat([df, pd.read_csv(df_name, index_col=0)])

df.to_csv('concat_dataset')
