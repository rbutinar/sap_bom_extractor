import pandas as pd
import os
import glob

data_dir = 'data'

# Preview all tables as CSV
stko = pd.read_csv('source_data/STKO.csv')
print('STKO:')
print(stko)
print('\n')

stpo = pd.read_csv('source_data/STPO.csv')
print('STPO:')
print(stpo)
print('\n')

mast = pd.read_csv('source_data/MAST.csv')
print('MAST:')
print(mast)
print('\n')
