import pandas as pd
from ..processing.tools import *


df = pd.read_csv('/eos/jeodpp/data/projects/REFOCUS/clamart/code/EuroCropsV2/data/cropcodemapping/eurocrops.csv')
to_sql_with_indexes(df,'cropmapping.eurocrops',index_cols='all')

df = pd.read_csv('/eos/jeodpp/data/projects/REFOCUS/clamart/code/EuroCropsV2/data/cropcodemapping/hcat4_agriprod_mapping.csv')
to_sql_with_indexes(df,'cropmapping.hcat4_agriprod_mapping',index_cols='all')


# df["usage_code"] = df["usage_code"].astype("Int64")
# df.loc[pd.isnull(df["usage_code"])]

