import pandas as pd

file_path = '/Users/luis/Documents/Github/excel-challenge/dataset/data.xlsx'
df = pd.read_excel(file_path)

print(df.head())