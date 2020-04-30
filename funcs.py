import pandas as pd
import numpy as np
import gc


def get_files_names(only_not_imported=False):
    '''retorna uma lista, com 4 listas de nomes de arquivos
    de rat (rat, viatura, efetivo, produtividade)'''
    import os,re    

    dir = os.getcwd()
    rats_files_names = os.listdir(dir+'/files/RAT')
    rat_pattern = re.compile('REDS_RAT_2')
    rat_viatura_pattern = re.compile('RAT_VIATURA')
    rat_efetivo_pattern = re.compile('RAT_EFETIVO')
    rat_produtividade_pattern = re.compile('RAT_Produtividade')

    rats_files = list()
    rats_viatura_files = list()
    rats_efetivo_files = list()
    rats_produtividade_files = list()

    if only_not_imported:
        df_imported_files = pd.read_sql_table('tbl_imported_files', 'sqlite:///gdo.db')
        rats_files_names = [file_name for file_name in rats_files_names if file_name not in df_imported_files['0'].values ]

    for file_name in rats_files_names:
        is_rat = rat_pattern.search(file_name) != None
        is_rat_viatura = rat_viatura_pattern.search(file_name) != None
        is_rat_efetivo = rat_efetivo_pattern.search(file_name) != None
        is_rat_produtividade = rat_produtividade_pattern.search(file_name) != None
        if is_rat:
            rats_files.append(file_name)
        elif is_rat_viatura:
            rats_viatura_files.append(file_name)
        elif is_rat_efetivo:
            rats_efetivo_files.append(file_name)
        elif is_rat_produtividade:
            rats_produtividade_files.append(file_name)
            
    rats_files.sort(key=lambda name: int(name[9:17]))
    rats_viatura_files.sort(key=lambda name: int(name[18:26]))
    rats_efetivo_files.sort(key=lambda name: int(name[18:26]))
    rats_produtividade_files.sort(key=lambda name: int(name[23:31]))
    return {
        'rat':rats_files,
        'ratv':rats_viatura_files,
        'rate':rats_efetivo_files,
        'ratp':rats_produtividade_files
    }


def get_df_classif():
    df_classif = pd.read_csv('files/classificadores.csv')    
    df_classif.set_index( df_classif['MUNICIPIO'] + " " + df_classif['VALIDADOR_TIPO'] + " " + df_classif['VALIDADOR'], inplace=True)    
    df_classif.fillna('', inplace=True)
    df_classif = df_classif.reset_index().drop_duplicates('index').set_index('index')
    return df_classif

def filter_23(df):
    return df[
        df['MUNICIPIO'].isin([
            'DIVINOPOLIS',
            'ITAUNA',
            'ITATIAIUCU',
            'CARMO DO CAJURU',
            'SAO GONCALO DO PARA',
            'CLAUDIO'
        ])
    ]


def read_files(files_path_names, df_rat=None, rat_data=True):
    if len( files_path_names) < 1:
        raise Exception('Não há arquivos csv novos para serem inseridos no banco de dados.')
    for i in range( len(files_path_names) ):    
        if i == 0:        
            df = pd.read_csv('files/RAT/' + files_path_names[i], error_bad_lines=False, sep='|')
            df = df.applymap(lambda x: x.strip() if type(x) == str else x)    
            if rat_data:
                df = filter_23(df)
            else:
                df = df[
                    df.iloc[:,0].isin(df_rat.index)
                ]
        else:
            df_aux = pd.read_csv('files/RAT/' + files_path_names[i], error_bad_lines=False, sep='|')
            df_aux = df_aux.applymap(lambda x: x.strip() if type(x) == str else x)
            if rat_data:
                df_aux = filter_23(df_aux)
            else:
                df_aux = df_aux[
                    df_aux.iloc[:,0].isin(df_rat.iloc[:,0].values)
                ]
            df = pd.concat([df, df_aux])
    return df


def data_rat_processing(df_rat):
    df_rat.drop_duplicates(subset='RAT.NUM_ATIVIDADE', keep='last', inplace=True)

    s_dta_in = df_rat['DTA_INICIO'] + " " + df_rat['HRA_INICIO']
    df_rat.loc[:,'DTA_HRA_INICIO_DT'] = pd.to_datetime( s_dta_in, format='%d/%m/%Y %H:%M', errors='coerce')
    del s_dta_in

    s_dta_ter = df_rat.loc[:,'DTA_TERMINO'] + " " + df_rat.loc[:,'HRA_TERMINO']
    df_rat.loc[:,'DTA_HRA_TERMINO_DT'] = pd.to_datetime( s_dta_ter, format='%d/%m/%Y %H:%M', errors='coerce')
    del s_dta_ter

    df_rat.loc[:,'TEMPO_DT'] = df_rat['DTA_HRA_TERMINO_DT'] - df_rat['DTA_HRA_INICIO_DT']

    df_rat.loc[:,'TEMPO_INT'] = df_rat['TEMPO_DT'].dt.total_seconds() / 60

    df_rat.loc[:,'DIA'] = df_rat.loc[:,'DTA_HRA_INICIO_DT'].dt.day
    
    df_rat.loc[:,'MES'] = df_rat.loc[:,'DTA_HRA_INICIO_DT'].dt.month

    df_rat.loc[:,'ANO'] = df_rat.loc[:,'DTA_HRA_INICIO_DT'].dt.year

    df_rat.loc[:,'DEZENA'] = np.select([
        df_rat['DTA_HRA_INICIO_DT'].dt.day <= 10,
        df_rat['DTA_HRA_INICIO_DT'].dt.day <= 20
    ],[1,2], default=3)


    for field in ['TEMPO_INT', 'DIA', 'MES', 'ANO', 'DEZENA']:
        df_rat[field].fillna(0, inplace=True)
        df_rat[field] = df_rat[field].astype('int32')

    cols_classif = [
        'MUNICIPIO',
        'LOGRADOURO',
        'DES_ENDERECO',
        'COMPLEMENTO_ENDERECO',
        'NOME_BAIRRO',
        'LOGRADOURO2',
        'DES_ENDERECO2'
    ]
    df_rat[cols_classif] = df_rat[cols_classif].apply(lambda col: col.str.upper())
    df_rat[cols_classif] = df_rat[cols_classif].fillna('')

    return df_rat


def classifica_setor(row, df_classif):
    mun = row['MUNICIPIO']
    if mun == 'CLAUDIO':        
        return 'CLAUDIO'
    elif mun == 'ITATIAIUCU':
        return 'LOURDES/ITATIAIUCU'
    elif mun in ('CARMO DO CAJURU', 'SAO GONCALO DO PARA'):             
        return 'CARMO DO CAJURU/SAO GONCALO DO PARA'    
    elif ( mun + " N_RAT " + row['RAT.NUM_ATIVIDADE'] ) in df_classif.index:
        return df_classif.loc[mun+" N_RAT "+row['RAT.NUM_ATIVIDADE'], 'SETOR']
    elif mun + ' BAIRRO ' + row['NOME_BAIRRO'] in df_classif.index:       
        return ( df_classif.loc[mun + ' BAIRRO ' + row['NOME_BAIRRO'].upper(), 'SETOR'] ).upper()
    elif mun + ' LOGRADOURO ' + row['LOGRADOURO'] in df_classif.index:
        return df_classif.loc[mun + ' LOGRADOURO ' + row['LOGRADOURO'].upper(), 'SETOR']
    elif mun + ' LOGRADOURO ' + row['DES_ENDERECO'] in df_classif.index:
        return df_classif.loc[mun + ' LOGRADOURO ' + row['DES_ENDERECO'].upper(), 'SETOR']
    elif mun + ' LOGRADOURO_NAO_CAD ' + row['LOGRADOURO2'] in df_classif.index:
        return df_classif.loc[mun + ' LOGRADOURO_NAO_CAD ' + row['LOGRADOURO2'].upper(), 'SETOR']
    elif mun + ' LOGRADOURO_NAO_CAD ' + row['DES_ENDERECO'] in df_classif.index:
        return df_classif.loc[mun + ' LOGRADOURO_NAO_CAD ' + row['DES_ENDERECO'].upper(), 'SETOR']
    elif mun + ' LOGRADOURO_NAO_CAD ' + row['LOGRADOURO2'] in df_classif.index:
        return df_classif.loc[mun + ' LOGRADOURO_NAO_CAD ' + row['LOGRADOURO2'].upper(), 'SETOR']
    elif mun + ' LOGRADOURO_NAO_CAD ' + row['DES_ENDERECO2'] in df_classif.index:
        return df_classif.loc[mun + ' LOGRADOURO_NAO_CAD ' + row['DES_ENDERECO2'].upper(), 'SETOR']
    elif mun + ' COMPLEMENTO_END ' + row['COMPLEMENTO_ENDERECO'] in df_classif.index:
        return df_classif.loc[mun + ' COMPLEMENTO_END ' + row['COMPLEMENTO_ENDERECO'].upper(), 'SETOR']
    elif ( mun + ' COMPLEMENTO_END ' + row['DES_ENDERECO'] ) in df_classif.index:
        return df_classif.loc[mun + ' COMPLEMENTO_END ' + row['DES_ENDERECO'], 'SETOR']    
    else:
        return 'other'


def classifica_cia(df_rat):
    conds=[
        df_rat['MUNICIPIO'].isin(['ITAUNA','ITATIAIUCU']),
        df_rat['SETOR'].isin(['HIPER CENTRO','BOM PASTOR','ALTO GOIAS']),
        df_rat['SETOR'].isin(['PLANALTO','SAO JOSE','CLAUDIO']),
        df_rat['SETOR'].isin(['NITEROI','PORTO VELHO','CARMO DO CAJURU/SAO GONCALO DO PARA']),
        
    ]
    res=['51 CIA','53 CIA','139 CIA','142 CIA']
    df_rat['CIA'] = np.select(conds,res,default='other')

