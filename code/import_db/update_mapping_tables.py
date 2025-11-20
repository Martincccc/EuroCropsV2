import pandas as pd
from utils.tools import to_sql_with_indexes


df = pd.read_csv('./data/cropcodemapping/eurocrops.csv')
to_sql_with_indexes(df,'cropmapping.eurocrops',index_cols='all')

df = pd.read_csv('./data/cropcodemapping/hcat4_agriprod_mapping.csv')
to_sql_with_indexes(df,'cropmapping.hcat4_agriprod_mapping',index_cols='all')

df = pd.read_csv('./data/cropcodemapping/hcat4_eagle_mapping.csv')
to_sql_with_indexes(df,'cropmapping.hcat4_eagle_mapping',index_cols='all')