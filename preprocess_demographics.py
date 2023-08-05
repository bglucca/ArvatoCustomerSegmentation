import pandas as pd
import numpy as np
import argparse

from utils.preprocessing import *

parser = argparse.ArgumentParser(description='Preprocess demographic data for clustering')

parser.add_argument('input_path', type = str, help = 'Raw CSV input Path')

parser.add_argument('output_path', type = str, help = 'Parquet file output Path')

args = parser.parse_args()

def demographic_preprocess_pipeline(input_path = args.input_path,
                                    output_path = args.output_path):

    '''
    Does the preprocessing of the demographic data after Explorations in the 01 and 02 notebooks.
    Exports data as Parquet to path specified in output_path'

    :param input_path: demographic data input file path
    :output_path: file export path
    '''
    print('Reading Data...')

    data = pd.read_csv(input_path, sep = ';')

    print('Renaming csv data columns to match documentation...')

    d19_cols_rename = {'D19_BANKEN_DIREKT': 'D19_BANKEN_DIREKT_RZ',
                        'D19_BANKEN_GROSS': 'D19_BANKEN_GROSS_RZ',
                        'D19_BANKEN_LOKAL': 'D19_BANKEN_LOKAL_RZ',
                        'D19_BANKEN_REST': 'D19_BANKEN_REST_RZ',
                        'D19_BEKLEIDUNG_GEH': 'D19_BEKLEIDUNG_GEH_RZ',
                        'D19_BEKLEIDUNG_REST': 'D19_BEKLEIDUNG_REST_RZ',
                        'D19_BILDUNG': 'D19_BILDUNG_RZ',
                        'D19_BIO_OEKO': 'D19_BIO_OEKO_RZ',
                        'D19_BUCH_CD': 'D19_BUCH_CD_RZ',
                        'D19_DIGIT_SERV': 'D19_DIGIT_SERV_RZ',
                        'D19_DROGERIEARTIKEL': 'D19_DROGERIEARTIKEL_RZ',
                        'D19_ENERGIE': 'D19_ENERGIE_RZ',
                        'D19_FREIZEIT': 'D19_FREIZEIT_RZ',
                        'D19_GARTEN': 'D19_GARTEN_RZ',
                        'D19_HANDWERK': 'D19_HANDWERK_RZ',
                        'D19_HAUS_DEKO': 'D19_HAUS_DEKO_RZ',
                        'D19_KINDERARTIKEL': 'D19_KINDERARTIKEL_RZ',
                        'D19_KONSUMTYP_MAX': 'D19_KONSUMTYP_MAX_RZ',
                        'D19_KOSMETIK': 'D19_KOSMETIK_RZ',
                        'D19_LEBENSMITTEL': 'D19_LEBENSMITTEL_RZ',
                        'D19_LETZTER_KAUF_BRANCHE': 'D19_LETZTER_KAUF_BRANCHE_RZ',
                        'D19_LOTTO': 'D19_LOTTO_RZ',
                        'D19_NAHRUNGSERGAENZUNG': 'D19_NAHRUNGSERGAENZUNG_RZ',
                        'D19_RATGEBER': 'D19_RATGEBER_RZ',
                        'D19_REISEN': 'D19_REISEN_RZ',
                        'D19_SAMMELARTIKEL': 'D19_SAMMELARTIKEL_RZ',
                        'D19_SCHUHE': 'D19_SCHUHE_RZ',
                        'D19_SONSTIGE': 'D19_SONSTIGE_RZ',
                        'D19_SOZIALES': 'D19_SOZIALES_RZ',
                        'D19_TECHNIK': 'D19_TECHNIK_RZ',
                        'D19_TELKO_MOBILE': 'D19_TELKO_MOBILE_RZ',
                        'D19_TELKO_REST': 'D19_TELKO_REST_RZ',
                        'D19_TIERARTIKEL': 'D19_TIERARTIKEL_RZ',
                        'D19_VERSAND_REST': 'D19_VERSAND_REST_RZ',
                        'D19_VERSICHERUNGEN': 'D19_VERSICHERUNGEN_RZ',
                        'D19_VOLLSORTIMENT': 'D19_VOLLSORTIMENT_RZ',
                        'D19_WEIN_FEINKOST': 'D19_WEIN_FEINKOST_RZ'}

    other_cols_rename = {'CAMEO_INTL_2015':'CAMEO_DEUINTL_2015',
                        'SOHO_KZ':'SOHO_FLAG',
                        'KK_KUNDENTYP':'D19_KK_KUNDENTYP',
                        'KBA13_CCM_1401_2500':'KBA13_CCM_1400_2500'}

    data.rename(columns = d19_cols_rename, inplace = True)
    
    data.rename(columns = other_cols_rename, inplace = True)

    print('Replacing NaNs...')
    
    data['CAMEO_DEUG_2015'] = data['CAMEO_DEUG_2015'].replace('X',np.nan)

    data['CAMEO_DEUINTL_2015'] = data['CAMEO_DEUINTL_2015'].replace('XX',np.nan)

    data['CAMEO_DEU_2015'] = data['CAMEO_DEU_2015'].replace('XX',np.nan)

    data['CAMEO_DEUG_2015'] = data['CAMEO_DEUG_2015'].fillna(-1).astype(int)

    data['CAMEO_DEUINTL_2015'] = data['CAMEO_DEUINTL_2015'].fillna(-1).astype(int)

    data['CAMEO_DEU_2015'] = data['CAMEO_DEU_2015'].fillna('-1')

    data = replace_nans(data)

    print('Removing irrelevant or undocumented columns...')

    cols_to_drop = ['GEBURTSJAHR',
                    'SOHO_FLAG',
                    'KBA13_HALTER_20',
                    'KBA13_HALTER_25',
                    'KBA13_HALTER_30',
                    'KBA13_HALTER_35',
                    'KBA13_HALTER_40',
                    'KBA13_HALTER_45',
                    'KBA13_HALTER_50',
                    'KBA13_HALTER_55',
                    'KBA13_HALTER_60',
                    'KBA13_HALTER_65',
                    'KBA13_HALTER_66',
                    'KBA13_CCM_1400_2500',
                    'KBA13_KMH_110',
                    'KBA13_KMH_140',
                    'KBA13_KMH_180',
                    'KBA13_KMH_210',
                    'KBA13_KMH_250',
                    'KBA13_KMH_251',
                    'ALTERSKATEGORIE_FEIN',
                    'LP_FAMILIE_FEIN',
                    'LP_LEBENSPHASE_FEIN',
                    'LP_STATUS_FEIN']
    
    not_found_cols = ['AKT_DAT_KL',
                    'DSL_FLAG',
                    'EINGEFUEGT_AM',
                    'EINGEZOGENAM_HH_JAHR',
                    'EXTSEL992',
                    'FIRMENDICHTE',
                    'GEMEINDETYP',
                    'HH_DELTA_FLAG',
                    'KBA13_BAUMAX',
                    'KBA13_GBZ',
                    'KBA13_HHZ',
                    'KOMBIALTER',
                    'KONSUMZELLE',
                    'MOBI_RASTER',
                    'RT_KEIN_ANREIZ',
                    'RT_SCHNAEPPCHEN',
                    'RT_UEBERGROESSE',
                    'STRUKTURTYP',
                    'UMFELD_ALT',
                    'UMFELD_JUNG',
                    'UNGLEICHENN_FLAG',
                    'VERDICHTUNGSRAUM',
                    'VHA',
                    'VHN',
                    'VK_DHT4A',
                    'VK_DISTANZ',
                    'VK_ZG11',
                    'KBA13_ANTG1',
                    'KBA13_ANTG2',
                    'KBA13_ANTG3',
                    'KBA13_ANTG4']
    
    # Manually setting to enable scripts to be run in whichever order is needed
    cols_empty_on_census = ['ALTER_KIND4',
                            'TITEL_KZ',
                            'ALTER_KIND3',
                            'ALTER_KIND2',
                            'ALTER_KIND1',
                            'AGER_TYP',
                            'D19_KK_KUNDENTYP',
                            'KBA05_BAUMAX',
                            'ALTER_HH']

    data.drop(columns = cols_to_drop + not_found_cols + cols_empty_on_census, inplace = True)

    print('Fixing specific columns...')

    data['OST_WEST_KZ'] = data['OST_WEST_KZ'].map({'O':0,'W':1})

    # CAMEO DEU specific fix
    unique_vals_cameo = data['CAMEO_DEU_2015'].dropna().unique()

    unique_vals_cameo = sorted(unique_vals_cameo)

    int_range = range(1, len(unique_vals_cameo) + 1)

    cameo_map = dict(zip(unique_vals_cameo, int_range))

    data['CAMEO_DEU_2015'] = data['CAMEO_DEU_2015'].map(cameo_map)

    # Binarizing
    data['ANZ_HH_TITEL'] = np.where((data['ANZ_HH_TITEL'] == 0) &\
                                   (data['ANZ_HH_TITEL'].notnull()), 0,
                                   np.where(data['ANZ_HH_TITEL'].notnull(), 1,np.nan))
    
    data['ANZ_KINDER'] = np.where((data['ANZ_KINDER'] == 0) &\
                                   (data['ANZ_KINDER'].notnull()), 0,
                                   np.where(data['ANZ_KINDER'].notnull(), 1,np.nan))

    print('Dropping too emtpy rows...')

    data = drop_empty_rows(data, threshold = 0.3)

    print('Running Imputations...')
    cat_cols = CENSUS_VAR_TYPES[CENSUS_VAR_TYPES['Type'].isin(['interval', 'nominal', 'binary'])]['Attribute'].values

    cat_cols = list(np.intersect1d(cat_cols, data.columns))

    # Imputing using mode (most frequent) for categorical
    for col in cat_cols:
        
        try:
            
            data[col].fillna(data[col].mode().values[0], inplace = True)

        except:

            print(col)

            raise

    # Imputing using mean for numerical
    num_cols = list_cols_by_type(data = data, var_type='numeric')

    for col in num_cols:

        data[col].fillna(data[col].mean(), inplace = True)
    
    print('Re-enconding D19 columns...')
    
    # Reencoding D19 cols
    data = reencode_d19_columns(data)

    print('Asserting NaNs were handled...')
    
    nan_count = data.isna().sum()

    if nan_count.sum() != 0:

        print('Columns with NaN values identified:')
        print(nan_count[nan_count > 0], end = '\n\n')

        raise ValueError('No NaNs should still be in the data. Handle the cases and run again')
    
    else:

        print('OK')

    print('Re-encoding types...')
    
    cols_to_use = np.intersect1d(data.columns, CENSUS_VAR_TYPES['Attribute'])

    type_dict = dict(zip(CENSUS_VAR_TYPES[CENSUS_VAR_TYPES['Attribute'].isin(cols_to_use)]['Attribute'],
                         CENSUS_VAR_TYPES[CENSUS_VAR_TYPES['Attribute'].isin(cols_to_use)]['dtype']))

    data = data.astype(type_dict)

    print('Exporting...')
    data.to_parquet(output_path)

    print('Done!')
    
if __name__ == '__main__':

    demographic_preprocess_pipeline()