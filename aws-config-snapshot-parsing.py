import json
from glob import glob
import pandas as pd


data = []
nan_value = float("NaN")

# Stage 1 - Read JSON files from a folder & attach that to a python list for processing
for file_name in glob('json/*.json'):
    with open(file_name) as f:
        data.append(json.load(f))

# Stage 2 - Flatten JSON into Pandas Dataframe 
df = pd.json_normalize(data, record_path=['configurationItems'])

# Sort Dataframe by resource-type (this is optional. have not tested for performance implications of sorting)
# df.sort_values(by=['resourceType'], inplace=True)

# Stage 3 - Group account-level dataframe by individual resource-types
grp = df.groupby('resourceType')

# Stage 4 - Loop through resource groups and convert those into CSV files
'''
Notes 
- Loop operation can be multi-threaded. a good option for handling it - concurrent.futures python library
- Dataframe can be saved as parquet file insteat of csv (ref: https://pandas.pydata.org/pandas-docs/version/1.1/reference/api/pandas.DataFrame.to_parquet.html)
'''

for name, group in grp:
    group.replace("", nan_value, inplace=True)
    group.dropna(how='all', axis=1, inplace=True)
    file_name = str(name).replace('::','-')
    group.to_csv('folder-name/'+str(file_name)+'.csv') # 'folder-name/' is a placeholder path
