import pandas as pd
import numpy as np

VALUES_PATH = 'data/raw/DIAS Attributes - Values 2017.xlsx'

NAN_VALS_FILE = pd.read_excel(VALUES_PATH,
                              header = 1,
                              usecols = 'B:F',
                              dtype = str,
                              sheet_name='Tabelle1_fixed')

# Based on the 02 Notebook, contains columns selected in subset
CENSUS_VAR_TYPES =  pd.read_csv('data/trusted/var_type_list.csv')

CENSUS_VAR_GROUP = pd.read_csv('data/trusted/var_group_list.csv')

def fetch_info_level_columns(df_att_info,df_data,lvl_name):

    all_names = df_att_info[df_att_info['Information level'] == lvl_name]['Attribute'].values

    names_in_data = np.intersect1d(df_data.columns, all_names)

    return names_in_data

def list_cols_by_type(data, type_frame = CENSUS_VAR_TYPES, var_type = 'interval'):

    col_list = type_frame[type_frame['Type'] == var_type]['Attribute'].values

    intersect = np.intersect1d(col_list, data.columns)

    return intersect

def generate_nan_map(var_attributes = NAN_VALS_FILE):

    '''
    Generates the a dictionary with the list of values by columns to be replaced by NaN in a Pandas friendly format.

    :param values_path: Path of the 'Values' Spreadsheet

    :return nan_val_map: Dictionary to be used in replacement
    '''

    nan_val_df = var_attributes.copy()

    nan_val_df[['Attribute','Description']] = nan_val_df[['Attribute','Description']].fillna(method='ffill')

    # Assuming, from manual inspection from the 'Values' Spreadsheet, that NaNs are represented with substrings in Meaning col
    nan_val_df = nan_val_df[nan_val_df['Meaning'].str.contains('unknown',regex=True,na  = False)]

    nan_val_df['Value'] = nan_val_df['Value'].str.replace('\s','', regex = True)

    nan_val_df['Value'] = nan_val_df['Value'].str.split(',')

    nan_val_map = dict(zip(nan_val_df['Attribute'], nan_val_df['Value']))

    # Creating a dictionary in a pandas friendly format for filling nans
    nested_nan_map = {}

    for i, (k, v) in enumerate(nan_val_map.items()):

        nested_nan_map[k] = {int(digit):np.nan for digit in v}

    nested_nan_map['CAMEO_DEU_2015'] = {'-1':np.nan}
    
    return nested_nan_map

def replace_nans(dataframe):
    '''
    Replace NaN Values in demographic data

    :param dataframe: demographic data DataFrame

    :return dataframe: DataFrame with NaN values
    '''

    nested_nan_map = generate_nan_map()

    dataframe = dataframe.replace(nested_nan_map)

    return dataframe

def drop_empty_rows(dataframe, threshold = 0.3):

    '''
    Drops Rows with NaN values in their columns bigger than a certain, arbitrary, proportional threshold.

    :param dataframe: DataFrame with rows to be analyzed
    :param threshold: Arbitrary threshold (Between 0 and 1)
    
    :return dataframe: dataframe without rows that didn't match the threshold
    '''

    if (0 > threshold) or (threshold > 1):

        raise ValueError('The threshold must be a value between 0 and 1 (proportional)')

    dataframe['_nan_count'] = dataframe.isna().sum(axis = 1)

    dataframe['_nan_prop'] = dataframe['_nan_count'] / dataframe.shape[1]

    dataframe = dataframe[dataframe['_nan_prop'] <= threshold].copy()

    dataframe.drop(columns = ['_nan_count','_nan_prop'], inplace = True)

    return dataframe

def reencode_d19_columns(dataframe, column_groups = CENSUS_VAR_GROUP):

    # Identifying columns automatically
    grid_cols_list = list(
                            fetch_info_level_columns(column_groups, dataframe, lvl_name = '125m x 125m Grid')
                          )

    grid_cols_list.remove('D19_LETZTER_KAUF_BRANCHE_RZ')

    d19_household_cols_list = fetch_info_level_columns(column_groups, dataframe, lvl_name = 'Household')

    d19_household_cols_list = [col for col in d19_household_cols_list\
                               if (col.startswith('D19')) and (col not in ['D19_KONSUMTYP','D19_KONSUMTYP_MAX_RZ'])]
    
    for col in grid_cols_list:

        grid_conditions = [dataframe[col] == 0,
                            dataframe[col].isin([1,2,3]),
                            dataframe[col].isin([4,5,6]),
                            dataframe[col] == 7]

        grid_choices = [0,1,2,3]

        dataframe[col] = np.select(grid_conditions, grid_choices, default = np.nan)
    
    for col in d19_household_cols_list:

        if 'DATUM' in col:
            
            dataframe[col] = np.where(dataframe[col] <= 5, 1,
                                np.where(dataframe[col] < 10, 2, 3))
            
        if 'ANZ' in col:
            
            anz_conditions = [dataframe[col] == 0, dataframe[col] <= 2, dataframe[col] <= 4, dataframe[col] <= 6]

            anz_choices = [0,1,2,3]

            dataframe[col] = np.select(condlist = anz_conditions, choicelist = anz_choices, default = np.nan)

        if 'QUOTE' in col:
            
            quote_conditions = [dataframe[col] == 0, dataframe[col] == 10]

            quote_choices = [0,2]

            dataframe[col] = np.select(condlist = quote_conditions, choicelist=quote_choices, default = 1)

    return dataframe
    



