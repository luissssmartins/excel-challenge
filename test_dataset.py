import pandas as pd

file_path = '/Users/luis/Documents/Github/excel-challenge/dataset/data.xlsx'
df = pd.read_excel(file_path)

def test_data(data_nasc, data_cadastro):
  print(f"Data de nascimento: {data_nasc}")
  print(f"Data de cadastro: {data_cadastro}")

for i in range(len(df)):
  test_data(df['Data Nasc.'][i], df['Data Cadastro cliente'][i])