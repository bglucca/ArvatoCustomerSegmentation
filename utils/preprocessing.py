import pandas as pd
import numpy as np

VALUES_PATH = 'data/raw/DIAS Attributes - Values 2017.xlsx'

# Based on the 01 Notebook
SELECTED_COLS_SUBSET = ['LNR',
                        'ANZ_HAUSHALTE_AKTIV',
                        'ANZ_HH_TITEL',
                        'ANZ_PERSONEN',
                        'ANZ_TITEL',
                        'KBA13_ANZAHL_PKW',
                        'MIN_GEBAEUDEJAHR',
                        'D19_BANKEN_DIREKT',
                        'D19_BANKEN_GROSS',
                        'D19_BANKEN_LOKAL',
                        'D19_BANKEN_REST',
                        'D19_BIO_OEKO',
                        'D19_DIGIT_SERV',
                        'D19_LEBENSMITTEL',
                        'D19_VOLLSORTIMENT',
                        'D19_VERSAND_REST',
                        'GEBAEUDETYP',
                        'KONSUMNAEHE',
                        'OST_WEST_KZ',
                        'WOHNLAGE',
                        'KBA05_MOTOR',
                        'KBA05_FRAU',
                        'KBA05_KRSZUL',
                        'KBA05_DIESEL',
                        'KBA05_KRSKLEIN',
                        'KBA05_KRSAQUOT',
                        'KBA05_MAXVORB',
                        'KBA05_MOTRAD',
                        'KBA05_MAXAH',
                        'KBA05_MODTEMP',
                        'KBA05_KRSOBER',
                        'KBA05_KRSVAN',
                        'KBA05_MAXHERST',
                        'KBA05_MAXBJ',
                        'ARBEIT',
                        'ORTSGR_KLS9',
                        'HH_EINKOMMEN_SCORE',
                        'D19_KONSUMTYP',
                        'D19_GESAMT_DATUM',
                        'D19_BANKEN_DATUM',
                        'D19_VERSAND_DATUM',
                        'D19_GESAMT_ONLINE_QUOTE_12',
                        'D19_BANKEN_ONLINE_QUOTE_12',
                        'D19_VERSAND_ONLINE_QUOTE_12',
                        'W_KEIT_KIND_HH',
                        'WOHNDAUER_2008',
                        'D19_GESAMT_ANZ_12',
                        'D19_VERSAND_ANZ_12',
                        'ANZ_KINDER',
                        'ANZ_STATISTISCHE_HAUSHALTE',
                        'STRUKTURTYP',
                        'CAMEO_DEUG_2015',
                        'CAMEO_DEU_2015',
                        'KBA05_ALTER1',
                        'KBA05_ALTER2',
                        'KBA05_ALTER3',
                        'KBA05_ALTER4',
                        'KBA05_ANHANG',
                        'KBA05_ANTG1',
                        'KBA05_ANTG2',
                        'KBA05_ANTG3',
                        'KBA05_ANTG4',
                        'ALTERSKATEGORIE_GROB',
                        'ANREDE_KZ',
                        'CJT_GESAMTTYP',
                        'FINANZ_MINIMALIST',
                        'FINANZ_SPARER',
                        'FINANZ_VORSORGER',
                        'FINANZ_ANLEGER',
                        'FINANZ_UNAUFFAELLIGER',
                        'FINANZ_HAUSBAUER',
                        'FINANZTYP',
                        'GFK_URLAUBERTYP',
                        'GREEN_AVANTGARDE',
                        'HEALTH_TYP',
                        'LP_LEBENSPHASE_GROB',
                        'LP_FAMILIE_GROB',
                        'LP_STATUS_GROB',
                        'NATIONALITAET_KZ',
                        'PRAEGENDE_JUGENDJAHRE',
                        'RETOURTYP_BK_S',
                        'SEMIO_SOZ',
                        'SEMIO_FAM',
                        'SEMIO_REL',
                        'SEMIO_MAT',
                        'SEMIO_VERT',
                        'SEMIO_LUST',
                        'SEMIO_ERL',
                        'SEMIO_KULT',
                        'SEMIO_RAT',
                        'SEMIO_KRIT',
                        'SEMIO_DOM',
                        'SEMIO_KAEM',
                        'SEMIO_PFLICHT',
                        'SEMIO_TRADV',
                        'SHOPPER_TYP',
                        'VERS_TYP',
                        'ZABEOTYP',
                        'CJT_KATALOGNUTZER',
                        'CJT_TYP_1',
                        'CJT_TYP_2',
                        'CJT_TYP_3',
                        'CJT_TYP_4',
                        'CJT_TYP_5',
                        'CJT_TYP_6',
                        'KBA13_SEG_MINIWAGEN',
                        'KBA13_CCM_2000',
                        'KBA13_KMH_0_140',
                        'KBA13_SEG_SONSTIGE',
                        'KBA13_VORB_0',
                        'KBA13_CCM_1200',
                        'PLZ8_ANTG2',
                        'KBA13_KMH_211',
                        'KBA13_CCM_1000',
                        'PLZ8_ANTG3',
                        'KBA13_VORB_1_2',
                        'KBA13_ALTERHALTER_61',
                        'KBA13_ALTERHALTER_30',
                        'KBA13_AUTOQUOTE',
                        'KBA13_SEG_KLEINWAGEN',
                        'KBA13_CCM_1600',
                        'KBA13_GBZ',
                        'KBA13_SEG_WOHNMOBILE',
                        'KBA13_CCM_2500',
                        'KBA13_SEG_GROSSRAUMVANS',
                        'KBA13_SEG_OBEREMITTELKLASSE',
                        'KBA13_KRSAQUOT',
                        'PLZ8_GBZ',
                        'KBA13_VORB_2',
                        'KBA13_SEG_OBERKLASSE',
                        'KBA13_BJ_2008',
                        'KBA13_ALTERHALTER_60',
                        'PLZ8_ANTG1',
                        'KBA13_SEG_MITTELKLASSE',
                        'KBA13_SEG_GELAENDEWAGEN',
                        'KBA13_BJ_2000',
                        'KBA13_KRSSEG_KLEIN',
                        'KBA13_CCM_3000',
                        'KBA13_SEG_UTILITIES',
                        'KBA13_SEG_MINIVANS',
                        'KBA13_VORB_1',
                        'KBA13_BJ_1999',
                        'PLZ8_ANTG4',
                        'KBA13_KRSSEG_OBER',
                        'KBA13_CCM_3001',
                        'KBA13_KRSSEG_VAN',
                        'KBA13_KRSZUL_NEU',
                        'KBA13_SEG_KOMPAKTKLASSE',
                        'KBA13_KMH_140_210',
                        'KBA13_SEG_SPORTWAGEN',
                        'KBA13_CCM_1400',
                        'KBA13_BAUMAX',
                        'PLZ8_BAUMAX',
                        'PLZ8_HHZ',
                        'KBA13_BJ_2009',
                        'KBA13_CCM_2501',
                        'KBA13_VORB_3',
                        'KBA13_CCM_1500',
                        'KBA13_BJ_2006',
                        'KBA13_HHZ',
                        'KBA13_CCM_1800',
                        'KBA13_SEG_KLEINST',
                        'KBA13_ALTERHALTER_45',
                        'KBA13_SEG_VAN',
                        'KBA13_BJ_2004',
                        'BALLRAUM',
                        'EWDICHTE',
                        'INNENSTADT',
                        'GEBAEUDETYP_RASTER',
                        'KKK',
                        'MOBI_REGIO',
                        'ONLINE_AFFINITAET']

# Based on the 02 Notebook, contains columns selected in subset
CENSUS_VAR_TYPES =  pd.read_csv('data/trusted/census_var_types.csv')

CENSUS_VAR_TYPES.loc[CENSUS_VAR_TYPES['Attribute'] == 'ANZ_HH_TITEL','Type'] = 'binary'

def generate_nan_map(values_path = VALUES_PATH):

    '''
    Generates the a dictionary with the list of values by columns to be replaced by NaN in a Pandas friendly format.

    :param values_path: Path of the 'Values' Spreadsheet

    :return nan_val_map: Dictionary to be used in replacement
    '''

    nan_val_df = pd.read_excel('data/raw/DIAS Attributes - Values 2017.xlsx', header = 1, usecols = 'B:F', dtype = str)

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
    
    # Manually setting columns that have other name in Data
    nested_nan_map['SOHO_KZ'] = {-1:np.nan}

    nested_nan_map['KK_KUNDENTYP'] = {-1:np.nan}

    nested_nan_map['KBA13_CCM_1401_2500'] = {-1:np.nan}

    return nested_nan_map

def replace_nans(dataframe):
    '''
    Replace NaN Values in demographic data

    :param dataframe: demographic data DataFrame

    :return dataframe: DataFrame with NaN values
    '''

    nested_nan_map = generate_nan_map()

    dataframe = dataframe.replace(nested_nan_map)

    # Specific Case
    dataframe['CAMEO_DEU_2015'] = dataframe['CAMEO_DEU_2015'].replace('-1',np.nan)

    return dataframe

def drop_empty_rows(dataframe, threshold = 0.2):

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

def reencode_d19_columns(dataframe):

    # Grid columns that remained in the subset
    grid_cols_list = ['D19_BANKEN_DIREKT',
                    'D19_BANKEN_GROSS',
                    'D19_BANKEN_LOKAL',
                    'D19_BANKEN_REST',
                    'D19_BIO_OEKO',
                    'D19_DIGIT_SERV',
                    'D19_LEBENSMITTEL',
                    'D19_VOLLSORTIMENT',
                    'D19_VERSAND_REST']
    
    for col in grid_cols_list:

        grid_conditions = [dataframe[col] == 0,
                            dataframe[col].isin([1,2,3]),
                            dataframe[col].isin([4,5,6]),
                            dataframe[col] == 7]

        grid_choices = [0,1,2,3]

        dataframe[col] = np.select(grid_conditions, grid_choices, default = np.nan)

    d19_household_cols_list = ['D19_BANKEN_DATUM',
                                'D19_BANKEN_ONLINE_QUOTE_12',
                                'D19_GESAMT_ANZ_12',
                                'D19_GESAMT_DATUM',
                                'D19_GESAMT_ONLINE_QUOTE_12',
                                'D19_KONSUMTYP',
                                'D19_VERSAND_ANZ_12',
                                'D19_VERSAND_DATUM',
                                'D19_VERSAND_ONLINE_QUOTE_12']
    
    for col in d19_household_cols_list:

        if 'DATUM' in col:
            
            dataframe[col] = np.where(dataframe[col] <= 5, 1,
                                np.where(dataframe[col] < 10, 2, 3))
            
        if 'ANZ' in col:
            
            anz_conditions = [dataframe[col] == 0, dataframe[col] <= 2, dataframe[col] <= 4, dataframe[col] <= 5]

            anz_choices = [0,1,2,3]

            dataframe[col] = np.select(condlist = anz_conditions, choicelist = anz_choices, default = np.nan)

        if 'QUOTE' in col:
            
            quote_conditions = [dataframe[col] == 0, dataframe[col] == 10]

            quote_choices = [0,2]

            dataframe[col] = np.select(condlist = quote_conditions, choicelist=quote_choices, default = 1)

    return dataframe
    



