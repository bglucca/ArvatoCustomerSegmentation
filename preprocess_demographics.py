import pandas as pd
import numpy as np
import argparse

from utils.preprocessing import *

parser = argparse.ArgumentParser(description='Preprocess demographic data for clustering')

parser.add_argument('input_path', type = str, help = 'Raw CSV input Path')

parser.add_argument('output_path', type = str, help = 'Parquet file output Path')

parser.add_argument('--is_customer', default=0, type = int, help = 'Flag (1 or 0) that indicates if is customer data or not. Defaults to 0 (No)')

args = parser.parse_args()

def demographic_preprocess_pipeline(input_path = args.input_path, output_path = args.output_path, is_customer_data = args.is_customer):

    '''
    Does the preprocessing of the demographic data after Explorations in the 01 and 02 notebooks.
    Exports data as Parquet to path specified in output_path

    :param input_path: demographic data input file path
    :output_path: file export path
    '''
    print('Reading Data...')

    data = pd.read_csv(input_path, sep = ';')

    print('Replacing NaNs...')
    
    data['CAMEO_DEUG_2015'] = data['CAMEO_DEUG_2015'].replace('X',np.nan)

    data['CAMEO_INTL_2015'] = data['CAMEO_INTL_2015'].replace('XX',np.nan)

    data['CAMEO_DEU_2015'] = data['CAMEO_DEU_2015'].replace('XX',np.nan)

    data['CAMEO_DEUG_2015'] = data['CAMEO_DEUG_2015'].fillna(-1).astype(int)

    data['CAMEO_INTL_2015'] = data['CAMEO_INTL_2015'].fillna(-1).astype(int)

    data['CAMEO_DEU_2015'] = data['CAMEO_DEU_2015'].fillna('-1')

    data = replace_nans(data)

    data['CAMEO_DEU_2015'] = data['CAMEO_DEU_2015'].replace('-1',np.nan)

    print('Subsetting...')

    if is_customer_data:

        SELECTED_COLS_SUBSET.extend(['CUSTOMER_GROUP','ONLINE_PURCHASE','PRODUCT_GROUP'])

    data = data[SELECTED_COLS_SUBSET].copy()

    print('Fixing specific columns...')

    data['OST_WEST_KZ'] = data['OST_WEST_KZ'].map({'O':0,'W':1})

    unique_vals_cameo = data['CAMEO_DEU_2015'].dropna().unique()

    unique_vals_cameo = sorted(unique_vals_cameo)

    int_range = range(1, len(unique_vals_cameo) + 1)

    cameo_map = dict(zip(unique_vals_cameo, int_range))

    data['CAMEO_DEU_2015'] = data['CAMEO_DEU_2015'].map(cameo_map)

    print('Running Simpler Imputations...')
    cat_cols = CENSUS_VAR_TYPES[CENSUS_VAR_TYPES['Type'].isin(['interval', 'nominal', 'binary'])]['Attribute'].values

    cat_cols = list(np.intersect1d(cat_cols, data.columns))

    # Imputing only simpler cases
    for col in cat_cols:

        nan_rate = data[col].isna().mean()

        if nan_rate <= 0.1:

            data[col].fillna(data[col].mode()[0], inplace = True)
        
        else:

            continue

    num_cols = CENSUS_VAR_TYPES[~CENSUS_VAR_TYPES['Type'].isin(['interval', 'nominal', 'binary'])]['Attribute'].values

    for col in num_cols:

        data[col].fillna(data[col].mean(), inplace = True)
    
    print('Exporting...')
    data.to_parquet(output_path)

    print('Done!')
    
if __name__ == '__main__':

    demographic_preprocess_pipeline()