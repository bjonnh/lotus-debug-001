#!/usr/bin/env python
# coding: utf-8

'''
Author: PMA
Contributor: AR
'''
import errno
import os
import sys
from chemosanitizer_functions import *
import numpy as np

# defining the command line arguments
try:
    input_file_path = sys.argv[1]
    ouput_file_path = sys.argv[2]
    smiles_column_header = sys.argv[3]
    cpus = int(sys.argv[4])

    print(f'Parsing tab separated file {input_file_path}'
          + f' with column: {smiles_column_header}'
          + f' as SMILES column. Paralelized on {cpus} cores.'
          + ' Proceeding to the validation, standardization, fragment choosing and uncharging of the ROMol object and returning the sanitized outputs in file :'
          + ouput_file_path)
except:
    print(
        '''Please add input and output file path as first and second argument, SMILES column header as third argument and finally the number of cpus you want to use.
        Example :
        python chemosanitizer.py ~/translatedStructureRdkit.tsv ./test.tsv structureTranslated 6''')
    sys.exit(1)

if __name__ == "__main__":
    myZip = gzip.open(input_file_path)
    df = pd.read_csv(myZip, sep='\t')

    if (len(df) == 1) and df.empty:
        df['structureTranslated'] = '[Pu]'
        print('your dataframe is empty, plutonium loaded')
    else:
        print('your dataframe is not empty :)')

    df = df[df[smiles_column_header].notnull()]
    df_chunks = np.array_split(df, cpus)
    f = CleaningFunc(smiles_column_header).f
    with multiprocessing.Pool(cpus) as pool:
        processed_df = pd.concat(pool.map(f, df_chunks), ignore_index=True)

    df_2 = processed_df.loc[:,
       ~processed_df.columns.isin(['ROMol', 'ROMolSanitized', 'ROMolSanitizedLargestFragment', 'flatROMol',
                                   'ROMolSanitizedLargestFragmentUncharged'])]

    output_path = os.path.dirname(ouput_file_path)

    if output_path != '' and not os.path.exists(output_path):
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
