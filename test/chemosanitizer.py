#!/usr/bin/env python
# coding: utf-8

'''
Author: PMA
Contributor: AR
'''
import errno
import os
from chemosanitizer_functions import *

# defining the command line arguments
try:
    input_file_path = sys.argv[1]
    ouput_file_path = sys.argv[2]
    smiles_column_header = sys.argv[3]
    cpus = sys.argv[4]

    print('Parsing tab separated file'
          + input_file_path
          + ' with column: '
          + smiles_column_header
          + ' as SMILES column.'
          + ' Parralelized on '
          + cpus
          + ' cores.'
          + ' Proceeding to the validation, standardization, fragment choosing and uncharging of the ROMol object and returning the sanitized outputs in file :'
          + ouput_file_path)
except:
    print(
        '''Please add input and output file path as first and second argument, SMILES column header as third argument and finally the number of cpus you want to use.
        Example :
        python chemosanitizer.py ~/translatedStructureRdkit.tsv ./test.tsv structureTranslated 6''')

if __name__ == "__main__":
    myZip = gzip.open(input_file_path)
    df = pd.read_csv(myZip, sep='\t')

    if (len(df) == 1) and (df.empty):
        df['structureTranslated'] = '[Pu]'
        print('your dataframe is empty, plutonium loaded')
    else:
        print('your dataframe is not empty :)')
    
    df.columns
    df.info()
    df = df[df[smiles_column_header].notnull()]

    # the full df is split and each subdf are treated sequentially as df > 900000 rows retruned errors
    # (parralel treatment of these subdf should improve performance)
    n = 20000  # chunk row size
    list_df = [df[i:i + n] for i in range(0, df.shape[0], n)]
    for i in range(0, len(list_df)):
        with multiprocessing.Pool(int(cpus)) as pool:
            list_df_2[i] = pool.map(long_cleaning_function, list_df[i], smiles_column_header)
            pool.close()
            pool.join()

list_df_2.drop('ROMol', axis=1, inplace=True)
list_df_2.drop('ROMolSanitized', axis=1, inplace=True)
list_df_2.drop('ROMolSanitizedLargestFragment', axis=1, inplace=True)
list_df_2.drop('flatROMol', axis=1, inplace=True)
list_df_2.drop('ROMolSanitizedLargestFragmentUncharged', axis=1, inplace=True)

df_2 = pd.concat(list_df_2)
df_2.info()

if not os.path.exists(os.path.dirname(ouput_file_path)):
    try:
        os.makedirs(os.path.dirname(ouput_file_path))
    except OSError as exc:  # Guard against race condition
        if exc.errno != errno.EEXIST:
            raise

df_2.to_csv(
    ouput_file_path,
    sep='\t',
    index=False,
    compression='gzip'
)
